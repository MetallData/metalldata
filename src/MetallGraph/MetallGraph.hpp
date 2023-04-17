#pragma once

#include <boost/container/vector.hpp>
#include <boost/container/string.hpp>

#include <ygm/container/set.hpp>
#include <ygm/container/map.hpp>

#include "MetallJsonLines.hpp"

namespace msg
{

using DistributedStringSet = ygm::container::set<std::string>;
using DistributedAdjList   = ygm::container::map<std::string, std::vector<std::string> >;

template <class T>
struct PointerGuard
{
    PointerGuard(T*& pref, T* obj)
    : ptr(&pref)
    {
      assert(pref == nullptr);

      pref = obj;
    }

    ~PointerGuard() { delete *ptr; *ptr = nullptr; }

  private:
    T** ptr;

    PointerGuard()                               = delete;
    PointerGuard(PointerGuard&&)                 = delete;
    PointerGuard(const PointerGuard&)            = delete;
    PointerGuard& operator=(PointerGuard&&)      = delete;
    PointerGuard& operator=(const PointerGuard&) = delete;
};

template<class T> PointerGuard(T*&, T*) -> PointerGuard<T>;

#if NOT_YET_AND_MAYBE_NEVER_WILL
struct ProcessDataMG
{
  DistributedStringSet distributedKeys;
};

std::unique_ptr<ProcessDataMG> mgState;

void commEdgeSrcCheck(std::string src, std::string tgt, std::size_t idx)
{
  assert(msg::mgState.get());

  DistributedStringSet&  keyset = msg::mgState->distributedKeys;
  ygm::comm&             w      = keyset.comm();
  const int              self   = w.rank();
  const int              dest   = keyset.owner(src);

  if (self == dest)
  {
    if (keyset.count_local())
      commEdgeTgtCheck(std::string tgt, std::size_t idx);
  }
  else
  {
    w.async();
  }
}

struct MetallGraphKeys : std::tuple<MetallString, MetallString, MetallString>
{
  using base = std::tuple<std::string, std::string, std::string>;
  using base::base;

  std::string_view nodes_key()        const { return std::get<0>(*this); }
  std::string_view edges_source_key() const { return std::get<1>(*this); }
  std::string_view edges_target_key() const { return std::get<2>(*this); }
};

#endif /* NOT_YET_AND_MAYBE_NEVER_WILL */
}

namespace
{
  struct CountDataMG
  {
    explicit
    CountDataMG(ygm::comm& comm)
    : distributedKeys(comm), edgecnt(0), nodecnt(0)
    {}

    msg::DistributedStringSet distributedKeys;
    std::size_t               edgecnt;
    std::size_t               nodecnt;

    static CountDataMG* ptr;
  };

  CountDataMG* CountDataMG::ptr = nullptr;

  struct ConnCompMG
  {
    explicit
    ConnCompMG(ygm::comm& comm)
    : distributedAdjList(comm)
    {}

    msg::DistributedAdjList distributedAdjList;

    static ConnCompMG* ptr;
  };

  ConnCompMG* ConnCompMG::ptr = nullptr;
}


namespace experimental
{

using MetallString = boost::container::basic_string< char,
                                                     std::char_traits<char>,
                                                     metall::manager::allocator_type<char>
                                                   >;

const MetallJsonLines::value_type&
getKey(const MetallJsonLines::value_type& val, std::string_view key)
{
  return val.as_object()[key];
}

std::string
to_string(const MetallJsonLines::value_type& val)
{
  return ::metall::container::experimental::json::serialize(val);
}

std::function<bool(const MetallJsonLines::value_type&)>
genKeysChecker(std::vector<std::string_view> keys)
{
  return [fields = std::move(keys)](const MetallJsonLines::value_type& val) -> bool
         {
           try
           {
             bool        incl = true;
             const auto& obj  = val.as_object();

             for (std::string_view fld : fields)
             {
               incl = incl && obj.contains(fld);
             }

             return incl;
           }
           catch (...)
           {
             return false;
           }
         };
}

std::function<MetallJsonLines::value_type(MetallJsonLines::value_type)>
genKeysGenerator( std::vector<std::string_view> edgeKeyFields,
                  std::vector<std::string_view> edgeKeysOrigin
                )
{
  const int numKeys = std::min(edgeKeyFields.size(), edgeKeysOrigin.size());

  return [edgeKeys = std::move(edgeKeyFields), keyOrigin = std::move(edgeKeysOrigin), numKeys]
         (MetallJsonLines::value_type val) -> MetallJsonLines::value_type
         {
           auto& obj = val.as_object();

           for (int i = 0; i < numKeys; ++i)
           {
             try
             {
               const auto& str    = obj[keyOrigin[i]].as_string();
               std::string keyval = std::to_string(i);

               keyval.append(std::string_view{&*str.begin(), str.size()});

               obj[edgeKeys[i]] = keyval;

               CountDataMG::ptr->distributedKeys.async_insert(keyval);
             }
             catch (...) {}
           }

           return val;
         };
}

struct MGCountSummary : std::tuple<std::size_t, std::size_t>
{
  using base = std::tuple<std::size_t, std::size_t>;
  using base::base;

  std::size_t nodes() const { return std::get<0>(*this); }
  std::size_t edges() const { return std::get<1>(*this); }

  boost::json::object asJson() const
  {
    boost::json::object res;

    res["nodes"] = nodes();
    res["edges"] = edges();

    return res;
  }
};

void persistKeys(MetallJsonLines& lines, std::string_view key, msg::DistributedStringSet& keyValues)
{
  keyValues.local_for_all( [&lines, key](const std::string& keyval) -> void
                           {
                             MetallJsonLines::value_type& val = lines.append_local();
                             auto&                        obj = val.emplace_object();

                             obj[key] = keyval;
                           }
                         );
}

struct MetallGraph
{
    using edge_list_type      = MetallJsonLines;
    using node_list_type      = MetallJsonLines;
    using key_store_type      = boost::container::vector<MetallString, metall::manager::allocator_type<MetallString> >;
    using metall_manager_type = MetallJsonLines::metall_manager_type;
    using filter_type         = MetallJsonLines::filter_type;

    MetallGraph(metall_manager_type& manager, ygm::comm& comm)
    : edgelst(manager, comm, edge_location_suffix),
      nodelst(manager, comm, node_location_suffix),
      keys(manager.get_local_manager().find<key_store_type>(keys_location_suffix).first)
    {
      checked_deref(keys, ERR_OPEN_KEYS);
    }

    edge_list_type&       edges()       { return edgelst; }
    edge_list_type const& edges() const { return edgelst; }

    node_list_type&       nodes()       { return nodelst; }
    node_list_type const& nodes() const { return nodelst; }

    std::string_view nodeKey()    const { return keys->at(NODE_KEY_IDX); }
    std::string_view edgeSrcKey() const { return keys->at(EDGE_SRCKEY_IDX); }
    std::string_view edgeTgtKey() const { return keys->at(EDGE_TGTKEY_IDX); }

    ImportSummary
    readVertexFiles(const std::vector<std::string>& files)
    {
      return nodelst.readJsonFiles(files, genKeysChecker({ nodeKey() }));
    }

    ImportSummary
    readEdgeFiles(const std::vector<std::string>& files, std::vector<std::string_view> autoKeys = {})
    {
      if (autoKeys.empty())
        return edgelst.readJsonFiles(files, genKeysChecker({ edgeSrcKey(), edgeTgtKey() }));

      msg::PointerGuard cntStateGuard{ CountDataMG::ptr, new CountDataMG{nodelst.comm()} };
      ImportSummary     res = edgelst.readJsonFiles( files,
                                                     genKeysChecker(autoKeys),
                                                     genKeysGenerator({ edgeSrcKey(), edgeTgtKey() }, autoKeys)
                                                   );

      persistKeys(nodelst, nodeKey(), CountDataMG::ptr->distributedKeys);
      return res;
    }

    static
    void createNew( metall_manager_type& manager,
                    ygm::comm& comm,
                    std::string_view node_key,
                    std::string_view edge_src_key,
                    std::string_view edge_tgt_key
                  )
    {
      std::string_view edge_location_suffix_v{edge_location_suffix};
      std::string_view node_location_suffix_v{node_location_suffix};

      MetallJsonLines::createNew(manager, comm, {edge_location_suffix_v, node_location_suffix_v});

      auto&           mgr = manager.get_local_manager();
      key_store_type& vec = checked_deref( mgr.construct<key_store_type>(keys_location_suffix)(mgr.get_allocator())
                                         , ERR_CONSTRUCT_KEYS
                                         );

      vec.emplace_back(MetallString(node_key.data(),     node_key.size(),     mgr.get_allocator()));
      vec.emplace_back(MetallString(edge_src_key.data(), edge_src_key.size(), mgr.get_allocator()));
      vec.emplace_back(MetallString(edge_tgt_key.data(), edge_tgt_key.size(), mgr.get_allocator()));
    }
/*
    std::tuple<std::size_t, std::size_t>
    count0(std::vector<filter_type> nfilt, std::vector<filter_type> efilt)
    {
      msg::DistributedStringSet  selectedKeys{ nodes.comm() };
      std::size_t                edgeCount = 0;

      msg::mgState = msg::ProcessDataMG{&selectedKeys};

      auto nodeAction = [&selectedKeys, nodeKeyTxt = nodeKey()]
                        (std::size_t, const xpr::MetallJsonLines::value_type& val)->void
                        {
                          selectedKeys.async_insert(to_string(getKey(val, nodeKeyTxt)));
                        };

      auto edgeAction = [&selectedKeys, edgeSrcKeyTxt = edgeSrcKey(), edgeTgtKeyTxt = edgeTgtKey()]
                        (std::size_t pos, const xpr::MetallJsonLines::value_type& val)->void
                        {
                          commEdgeSrcCheck( to_string(getKey(val, edgeSrcKeyTxt)),
                                            to_string(getKey(val, edgeTgtKeyTxt)),
                                            pos
                                          );
                        };
    }
*/

    MGCountSummary
    count(std::vector<filter_type> nfilt, std::vector<filter_type> efilt)
    {
      msg::PointerGuard cntStateGuard{ CountDataMG::ptr, new CountDataMG{nodelst.comm()} };

      auto nodeAction = [nodeKeyTxt = nodeKey()]
                        (std::size_t, const MetallJsonLines::value_type& val)->void
                        {
                          msg::DistributedStringSet& keyStore = CountDataMG::ptr->distributedKeys;

                          keyStore.async_insert(to_string(getKey(val, nodeKeyTxt)));

                          ++CountDataMG::ptr->nodecnt;
                        };

      nodelst.filter(std::move(nfilt)).forAllSelected(nodeAction);

      // \todo
      //   this version only counts the presence of src and tgt vertex of an edge.
      //   To mark the actual edge, we need to add (owner, index) to the msg, so that the
      //   target vertex owner can notify the edge owner of its inclusion.
      auto edgeAction = [edgeSrcKeyTxt = edgeSrcKey(), edgeTgtKeyTxt = edgeTgtKey()]
                        (std::size_t pos, const MetallJsonLines::value_type& val)->void
                        {
                          msg::DistributedStringSet& keyStore = CountDataMG::ptr->distributedKeys;
                          auto commEdgeSrcCheck = [](const std::string& srckey, const std::string& tgtkey)
                                                  {
                                                    msg::DistributedStringSet& keyStore = CountDataMG::ptr->distributedKeys;
                                                    auto commEdgeTgtCheck = [](const std::string&)
                                                                            {
                                                                              ++CountDataMG::ptr->edgecnt;
                                                                            };

                                                    keyStore.async_exe_if_contains(tgtkey, commEdgeTgtCheck);
                                                  };

                          keyStore.async_exe_if_contains( to_string(getKey(val, edgeSrcKeyTxt)),
                                                          commEdgeSrcCheck,
                                                          to_string(getKey(val, edgeTgtKeyTxt))
                                                        );
                        };

      edgelst.filter(std::move(efilt)).forAllSelected(edgeAction);

      const std::size_t totalNodes = CountDataMG::ptr->distributedKeys.size();
      const std::size_t totalEdges = nodelst.comm().all_reduce_sum(CountDataMG::ptr->edgecnt);

      return { totalNodes, totalEdges };
    }

    ygm::comm& comm() { return nodelst.comm(); }

    MGCountSummary
    connectedComponents(std::vector<filter_type> nfilt, std::vector<filter_type> efilt)
    {
      msg::PointerGuard cntStateGuard{ ConnCompMG::ptr, new ConnCompMG{comm()} };

      auto nodeAction =
        [nodeKeyTxt = nodeKey()]
        (std::size_t, const MetallJsonLines::value_type& val)->void
        {
          std::string vertex = to_string(getKey(val, nodeKeyTxt));

          ConnCompMG::ptr->distributedAdjList.async_insert_if_missing( vertex, std::vector<std::string>{} );
        };

      nodelst.filter(std::move(nfilt)).forAllSelected(nodeAction);

      auto edgeAction =
        [edgeSrcKeyTxt = edgeSrcKey(), edgeTgtKeyTxt = edgeTgtKey()]
        (std::size_t pos, const MetallJsonLines::value_type& val)->void
        {
          msg::DistributedAdjList& adjList = ConnCompMG::ptr->distributedAdjList;

          auto commEdgeTgtCheck = [](const std::string& tgtkey, const std::vector<std::string>&, const std::string& srckey)
          {
            auto commEdgeSrcCheck =
              [](const std::string& /*srckey*/, std::vector<std::string>& edges, std::string tgtkey)
              {
                edges.emplace_back(std::move(tgtkey));
              };

            ConnCompMG::ptr->distributedAdjList.async_visit_if_exists(srckey, commEdgeSrcCheck, tgtkey);
          };

          // check first target, if in -> add edge to adjecency list at src
          adjList.async_visit_if_exists( to_string(getKey(val, edgeTgtKeyTxt)),
                                         commEdgeTgtCheck,
                                         to_string(getKey(val, edgeSrcKeyTxt))
                                       );
        };

      edgelst.filter(std::move(efilt)).forAllSelected(edgeAction);

      comm().barrier();

      {
        // from Roger's code
        // https://lc.llnl.gov/gitlab/metall/ygm-reddit/-/blob/main/bench/reddit_components_labelprop.cpp

        ygm::container::map<std::string, std::string> map_cc{comm()};
        ygm::container::map<std::string, std::string> active{comm()};
        ygm::container::map<std::string, std::string> next_active{comm()};

        static auto& s_next_active = next_active;
        static auto& s_map_cc      = map_cc;
        static auto& s_adj_list    = ConnCompMG::ptr->distributedAdjList;

        //
        // Init map_cc
        s_adj_list.for_all(
          [&map_cc, &active](const std::string& vertex, const std::vector<std::string>&) -> void
          {
            map_cc.async_insert(vertex, vertex);
            active.async_insert(vertex, vertex);
          });

        comm().barrier();

        while (active.size() > 0)
        {
          // comm().cout0("active.size() = ", active.size());
          active.for_all(
            [](const std::string& vertex, const std::string& cc_id) -> void
            {
              s_adj_list.async_visit(
                vertex,
                []( const std::string& vertex,
                    const std::vector<std::string>& adj,
                    const std::string& cc_id
                  ) -> void
                {
                  for (const auto& neighbor : adj)
                  {
                    if (cc_id < neighbor)
                    {
                      s_map_cc.async_visit(
                        neighbor,
                        [](const std::string& n, std::string& ncc, const std::string& cc_id) -> void
                        {
                          if (cc_id < ncc)
                          {
                            ncc = cc_id;
                            s_next_active.async_reduce(n, cc_id,
                              [](const std::string& a, const std::string& b) -> const std::string&
                              {
                                return std::min(a, b);
                              } );
                          }
                        },
                        cc_id );
                    }
                  }
                },
                cc_id );
            } );
          comm().barrier();
          active.clear();
          active.swap(next_active);
        }
      }

      // \todo revise returns...
      const std::size_t totalNodes = ConnCompMG::ptr->distributedAdjList.size();
      const std::size_t totalEdges = 0;

      return { totalNodes, totalEdges };
    }


    static
    void checkState( metall_manager_type& manager,
                     ygm::comm& comm
                   )
    {
      std::string_view edge_location_suffix_v{edge_location_suffix};
      std::string_view node_location_suffix_v{node_location_suffix};

      MetallJsonLines::checkState(manager, comm, {edge_location_suffix_v, node_location_suffix_v});

      auto&           mgr = manager.get_local_manager();

      key_store_type& vec = checked_deref( mgr.find<key_store_type>(keys_location_suffix).first
                                         , ERR_OPEN_KEYS
                                         );

      if (vec.size() != 3) throw std::runtime_error{ ERR_OPEN_KEYS };
      //~ if (node_key.size())     check_equality(node_key, nodeKey());
      //~ if (edge_src_key.size()) check_equality(edge_src_key, edgeSrcKey());
      //~ if (edge_tgt_key.size()) check_equality(edge_tgt_key, edgeTgtKey());
    }

  private:
    edge_list_type  edgelst;
    node_list_type  nodelst;
    key_store_type* keys = nullptr;

    static constexpr const char* const edge_location_suffix = "edges";
    static constexpr const char* const node_location_suffix = "nodes";
    static constexpr const char* const keys_location_suffix = "keys";

    static constexpr const char* const ERR_CONSTRUCT_KEYS = "unable to construct MetallGraph::keys object";
    static constexpr const char* const ERR_OPEN_KEYS      = "unable to open MetallGraph::keys object";

    static constexpr std::size_t NODE_KEY_IDX    = 0;
    static constexpr std::size_t EDGE_SRCKEY_IDX = NODE_KEY_IDX    + 1;
    static constexpr std::size_t EDGE_TGTKEY_IDX = EDGE_SRCKEY_IDX + 1;
};

}
