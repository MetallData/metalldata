#pragma once

#include <boost/container/string.hpp>
#include <boost/container/vector.hpp>

#include <ygm/container/map.hpp>
#include <ygm/container/set.hpp>

#include "MetallJsonLines.hpp"

namespace msg {

using distributed_string_set = ygm::container::set<std::string>;
using distributed_adj_list =
    ygm::container::map<std::string, std::vector<std::string> >;

template <class T>
struct ptr_guard {
  ptr_guard(T*& pref, T* obj) : ptr(&pref) {
    assert(pref == nullptr);

    pref = obj;
  }

  ~ptr_guard() {
    delete *ptr;
    *ptr = nullptr;
  }

 private:
  T** ptr;

  ptr_guard()                            = delete;
  ptr_guard(ptr_guard&&)                 = delete;
  ptr_guard(const ptr_guard&)            = delete;
  ptr_guard& operator=(ptr_guard&&)      = delete;
  ptr_guard& operator=(const ptr_guard&) = delete;
};

template <class T>
ptr_guard(T*&, T*) -> ptr_guard<T>;

#if NOT_YET_AND_MAYBE_NEVER_WILL
struct ProcessDataMG {
  distributed_string_set distributedKeys;
};

std::unique_ptr<ProcessDataMG> mgState;

void commEdgeSrcCheck(std::string src, std::string tgt, std::size_t idx) {
  assert(msg::mgState.get());

  distributed_string_set& keyset = msg::mgState->distributedKeys;
  ygm::comm&              w      = keyset.comm();
  const int               self   = w.rank();
  const int               dest   = keyset.owner(src);

  if (self == dest) {
    if (keyset.count_local())
      commEdgeTgtCheck(std::string tgt, std::size_t idx);
  } else {
    w.async();
  }
}

struct metall_graphKeys
    : std::tuple<metall_string, metall_string, metall_string> {
  using base = std::tuple<std::string, std::string, std::string>;
  using base::base;

  std::string_view nodes_key() const { return std::get<0>(*this); }
  std::string_view edges_source_key() const { return std::get<1>(*this); }
  std::string_view edges_target_key() const { return std::get<2>(*this); }
};

#endif /* NOT_YET_AND_MAYBE_NEVER_WILL */
}  // namespace msg

namespace {
struct count_data_mg {
  explicit count_data_mg(ygm::comm& comm)
      : distributedKeys(comm), edgecnt(0), nodecnt(0) {}

  msg::distributed_string_set distributedKeys;
  std::size_t                 edgecnt;
  std::size_t                 nodecnt;

  static count_data_mg* ptr;
};

count_data_mg* count_data_mg::ptr = nullptr;

struct conn_comp_mg {
  explicit conn_comp_mg(ygm::comm& comm) : distributedAdjList(comm) {}

  msg::distributed_adj_list distributedAdjList;

  static conn_comp_mg* ptr;
};

conn_comp_mg* conn_comp_mg::ptr = nullptr;
}  // namespace

namespace experimental {

using metall_string =
    boost::container::basic_string<char, std::char_traits<char>,
                                   metall::manager::allocator_type<char> >;

metall_json_lines::accessor_type get_key(metall_json_lines::accessor_type val,
                                         std::string_view                 key) {
  assert(val.is_object());
  return val.as_object()[key];
}

std::string to_string(const boost::json::value& val) {
  std::stringstream stream;

  stream << val;
  return stream.str();
}

std::string to_string(const metall_json_lines::accessor_type& valacc) {
  return to_string(json_bento::value_to<boost::json::value>(valacc));
}

std::function<bool(const boost::json::value&)> gen_keys_checker(
    std::vector<std::string_view> keys) {
  return [fields = std::move(keys)](const boost::json::value& val) -> bool {
    try {
      bool        incl = true;
      const auto& obj  = val.as_object();

      for (std::string_view fld : fields) {
        boost::json::string_view fldvw(&*fld.begin(), fld.size());

        incl = incl && obj.contains(fldvw);
      }

      return incl;
    } catch (...) {
      return false;
    }
  };
}

std::function<boost::json::value(boost::json::value)> gen_keys_generator(
    std::vector<std::string_view> edgeKeyFields,
    std::vector<std::string_view> edgeKeysOrigin) {
  const int numKeys = std::min(edgeKeyFields.size(), edgeKeysOrigin.size());

  return [edgeKeys  = std::move(edgeKeyFields),
          keyOrigin = std::move(edgeKeysOrigin),
          numKeys](boost::json::value val) -> boost::json::value {
    auto& obj = val.as_object();

    for (int i = 0; i < numKeys; ++i) {
      try {
        std::string_view         key = keyOrigin[i];
        boost::json::string_view keyVw(&*key.begin(), key.size());
        std::string              keyval = to_string(obj[keyVw]);

        keyval.push_back('@');
        keyval.append(key);

        boost::json::string_view edgeKeyVw(&*edgeKeys[i].begin(),
                                           edgeKeys[i].size());

        obj[edgeKeyVw] = keyval;

        count_data_mg::ptr->distributedKeys.async_insert(keyval);
      } catch (...) {
      }
    }

    return val;
  };
}

struct mg_count_summary : std::tuple<std::size_t, std::size_t> {
  using base = std::tuple<std::size_t, std::size_t>;
  using base::base;

  std::size_t nodes() const { return std::get<0>(*this); }
  std::size_t edges() const { return std::get<1>(*this); }

  boost::json::object asJson() const {
    boost::json::object res;

    res["nodes"] = nodes();
    res["edges"] = edges();

    return res;
  }
};

void persist_keys(metall_json_lines& lines, std::string_view key,
                  msg::distributed_string_set& keyValues) {
  keyValues.local_for_all([&lines, key](const std::string& keyval) -> void {
    metall_json_lines::accessor_type val = lines.append_local();
    auto                             obj = val.emplace_object();

    obj[key] = keyval;
  });
}

struct metall_graph {
  using edge_list_type = metall_json_lines;
  using node_list_type = metall_json_lines;
  using key_store_type =
      boost::container::vector<metall_string,
                               metall::manager::allocator_type<metall_string> >;
  using metall_manager_type = metall_json_lines::metall_manager_type;
  using filter_type         = metall_json_lines::filter_type;

  metall_graph(metall_manager_type& manager, ygm::comm& comm)
      : edgelst(manager, comm, edge_location_suffix),
        nodelst(manager, comm, node_location_suffix),
        keys(manager.get_local_manager()
                 .find<key_store_type>(keys_location_suffix)
                 .first) {
    checked_deref(keys, ERR_OPEN_KEYS);
  }

  edge_list_type&       edges() { return edgelst; }
  edge_list_type const& edges() const { return edgelst; }

  node_list_type&       nodes() { return nodelst; }
  node_list_type const& nodes() const { return nodelst; }

  std::string_view nodeKey() const { return keys->at(NODE_KEY_IDX); }
  std::string_view edgeSrcKey() const { return keys->at(EDGE_SRCKEY_IDX); }
  std::string_view edgeTgtKey() const { return keys->at(EDGE_TGTKEY_IDX); }

  import_summary read_vertex_files(const std::vector<std::string>& files) {
    return nodelst.read_json_files(files, gen_keys_checker({nodeKey()}));
  }

  import_summary read_edge_files(const std::vector<std::string>& files,
                                 std::vector<std::string_view> autoKeys = {}) {
    if (autoKeys.empty())
      return edgelst.read_json_files(
          files, gen_keys_checker({edgeSrcKey(), edgeTgtKey()}));

    msg::ptr_guard cntStateGuard{count_data_mg::ptr,
                                 new count_data_mg{nodelst.comm()}};
    import_summary res = edgelst.read_json_files(
        files, gen_keys_checker(autoKeys),
        gen_keys_generator({edgeSrcKey(), edgeTgtKey()}, autoKeys));

    comm().barrier();
    persist_keys(nodelst, nodeKey(), count_data_mg::ptr->distributedKeys);
    return res;
  }

  static void create_new(metall_manager_type& manager, ygm::comm& comm,
                         std::string_view node_key,
                         std::string_view edge_src_key,
                         std::string_view edge_tgt_key) {
    std::string_view edge_location_suffix_v{edge_location_suffix};
    std::string_view node_location_suffix_v{node_location_suffix};

    metall_json_lines::create_new(
        manager, comm, {edge_location_suffix_v, node_location_suffix_v});

    auto&           mgr = manager.get_local_manager();
    key_store_type& vec =
        checked_deref(mgr.construct<key_store_type>(keys_location_suffix)(
                          mgr.get_allocator()),
                      ERR_CONSTRUCT_KEYS);

    vec.emplace_back(
        metall_string(node_key.data(), node_key.size(), mgr.get_allocator()));
    vec.emplace_back(metall_string(edge_src_key.data(), edge_src_key.size(),
                                   mgr.get_allocator()));
    vec.emplace_back(metall_string(edge_tgt_key.data(), edge_tgt_key.size(),
                                   mgr.get_allocator()));
  }
  /*
      std::tuple<std::size_t, std::size_t>
      count0(std::vector<filter_type> nfilt, std::vector<filter_type> efilt)
      {
        msg::distributed_string_set  selectedKeys{ nodes.comm() };
        std::size_t                edgeCount = 0;

        msg::mgState = msg::ProcessDataMG{&selectedKeys};

        auto nodeAction = [&selectedKeys, nodeKeyTxt = nodeKey()]
                          (std::size_t, const
     xpr::metall_json_lines::value_type& val)->void
                          {
                            selectedKeys.async_insert(to_string(get_key(val,
     nodeKeyTxt)));
                          };

        auto edgeAction = [&selectedKeys, edgeSrcKeyTxt = edgeSrcKey(),
     edgeTgtKeyTxt = edgeTgtKey()] (std::size_t pos, const
     xpr::metall_json_lines::value_type& val)->void
                          {
                            commEdgeSrcCheck( to_string(get_key(val,
     edgeSrcKeyTxt)), to_string(get_key(val, edgeTgtKeyTxt)), pos
                                            );
                          };
      }
  */

  mg_count_summary count(std::vector<filter_type> nfilt,
                         std::vector<filter_type> efilt) {
    assert(count_data_mg::ptr == nullptr);

    msg::ptr_guard cntStateGuard{count_data_mg::ptr,
                                 new count_data_mg{nodelst.comm()}};
    int            rank = comm().rank();

    auto nodeAction = [nodeKeyTxt = nodeKey(), rank](
                          std::size_t,
                          const metall_json_lines::accessor_type& val) -> void {
      assert(count_data_mg::ptr != nullptr);

      msg::distributed_string_set& keyStore =
          count_data_mg::ptr->distributedKeys;

      std::string thekey = to_string(get_key(val, nodeKeyTxt));
      keyStore.async_insert(thekey);

      ++count_data_mg::ptr->nodecnt;
    };

    nodelst.filter(std::move(nfilt)).for_all_selected(nodeAction);
    comm().barrier();

    // \todo
    //   this version only counts the presence of src and tgt vertex of an edge.
    //   To mark the actual edge, we need to add (owner, index) to the msg, so
    //   that the target vertex owner can notify the edge owner of its
    //   inclusion.
    auto edgeAction = [edgeSrcKeyTxt = edgeSrcKey(),
                       edgeTgtKeyTxt = edgeTgtKey()](
                          std::size_t                             pos,
                          const metall_json_lines::accessor_type& val) -> void {
      msg::distributed_string_set& keyStore =
          count_data_mg::ptr->distributedKeys;
      auto commEdgeSrcCheck = [](const std::string& srckey,
                                 const std::string& tgtkey) {
        msg::distributed_string_set& keyStore =
            count_data_mg::ptr->distributedKeys;
        auto commEdgeTgtCheck = [](const std::string&) {
          ++count_data_mg::ptr->edgecnt;
        };

        keyStore.async_exe_if_contains(tgtkey, commEdgeTgtCheck);
      };

      keyStore.async_exe_if_contains(to_string(get_key(val, edgeSrcKeyTxt)),
                                     commEdgeSrcCheck,
                                     to_string(get_key(val, edgeTgtKeyTxt)));
    };

    edgelst.filter(std::move(efilt)).for_all_selected(edgeAction);
    comm().barrier();

    const std::size_t totalNodes = count_data_mg::ptr->distributedKeys.size();
    const std::size_t totalEdges =
        nodelst.comm().all_reduce_sum(count_data_mg::ptr->edgecnt);

    return {totalNodes, totalEdges};
  }

  ygm::comm& comm() { return nodelst.comm(); }

  std::size_t connected_components(std::vector<filter_type> nfilt,
                                   std::vector<filter_type> efilt) {
    msg::ptr_guard cntStateGuard{conn_comp_mg::ptr, new conn_comp_mg{comm()}};

    auto nodeAction = [nodeKeyTxt = nodeKey()](
                          std::size_t,
                          const metall_json_lines::accessor_type& val) -> void {
      std::string vertex = to_string(get_key(val, nodeKeyTxt));

      conn_comp_mg::ptr->distributedAdjList.async_insert_if_missing(
          vertex, std::vector<std::string>{});
    };

    nodelst.filter(std::move(nfilt)).for_all_selected(nodeAction);
    comm().barrier();

    auto edgeAction = [edgeSrcKeyTxt = edgeSrcKey(),
                       edgeTgtKeyTxt = edgeTgtKey()](
                          std::size_t                             pos,
                          const metall_json_lines::accessor_type& val) -> void {
      msg::distributed_adj_list& adjList =
          conn_comp_mg::ptr->distributedAdjList;

      auto commEdgeTgtCheck = [](const std::string& tgtkey,
                                 const std::vector<std::string>&,
                                 const std::string& srckey) {
        auto commEdgeSrcCheck =
            [](const std::string& /*srckey*/, std::vector<std::string>& edges,
               std::string tgtkey) { edges.emplace_back(std::move(tgtkey)); };

        conn_comp_mg::ptr->distributedAdjList.async_visit_if_exists(
            srckey, commEdgeSrcCheck, tgtkey);
        conn_comp_mg::ptr->distributedAdjList.async_visit_if_exists(
            tgtkey, commEdgeSrcCheck, srckey);
      };

      // check first target, if in then add edges (src->tgt and tgt->src) to
      // adjecency list at src
      //   we assume a
      adjList.async_visit_if_exists(to_string(get_key(val, edgeTgtKeyTxt)),
                                    commEdgeTgtCheck,
                                    to_string(get_key(val, edgeSrcKeyTxt)));
    };

    edgelst.filter(std::move(efilt)).for_all_selected(edgeAction);

    comm().barrier();
    static std::size_t localRoots = 0;

    {
      // from Roger's code
      // https://lc.llnl.gov/gitlab/metall/ygm-reddit/-/blob/main/bench/reddit_components_labelprop.cpp

      ygm::container::map<std::string, std::string> map_cc{comm()};
      ygm::container::map<std::string, std::string> active{comm()};
      ygm::container::map<std::string, std::string> next_active{comm()};

      // \todo could be factored into a class similar to conn_comp_mg
      static auto& s_next_active = next_active;
      static auto& s_map_cc      = map_cc;
      static auto& s_adj_list    = conn_comp_mg::ptr->distributedAdjList;

      //
      // Init map_cc
      s_adj_list.for_all(
          [&map_cc, &active](const std::string& vertex,
                             const std::vector<std::string>&) -> void {
            map_cc.async_insert(vertex, vertex);
            active.async_insert(vertex, vertex);
          });

      comm().barrier();

      while (active.size() > 0) {
        // comm().cout0("active.size() = ", active.size());
        active.for_all([](const std::string& vertex,
                          const std::string& cc_id) -> void {
          s_adj_list.async_visit(
              vertex,
              [](const std::string& vertex, const std::vector<std::string>& adj,
                 const std::string& cc_id) -> void {
                for (const auto& neighbor : adj) {
                  if (cc_id < neighbor) {
                    s_map_cc.async_visit(
                        neighbor,
                        [](const std::string& n, std::string& ncc,
                           const std::string& cc_id) -> void {
                          if (cc_id < ncc) {
                            ncc = cc_id;
                            s_next_active.async_reduce(
                                n, cc_id,
                                [](const std::string& a,
                                   const std::string& b) -> const std::string& {
                                  return std::min(a, b);
                                });
                          }
                        },
                        cc_id);
                  }
                }
              },
              cc_id);
        });
        comm().barrier();
        active.clear();
        active.swap(next_active);
      }

      localRoots = 0;

      // \todo should be local_for_all??
      s_map_cc.for_all(
          [](const std::string& lhs, const std::string& rhs) -> void {
            localRoots += (lhs == rhs);

            //~ if (lhs == rhs)
            //~ std::cerr << lhs << std::flush;
          });
    }

    const std::size_t totalRoots = nodelst.comm().all_reduce_sum(localRoots);

    return totalRoots;
  }

  static void check_state(metall_manager_type& manager, ygm::comm& comm) {
    std::string_view edge_location_suffix_v{edge_location_suffix};
    std::string_view node_location_suffix_v{node_location_suffix};

    metall_json_lines::check_state(
        manager, comm, {edge_location_suffix_v, node_location_suffix_v});

    auto& mgr = manager.get_local_manager();

    key_store_type& vec = checked_deref(
        mgr.find<key_store_type>(keys_location_suffix).first, ERR_OPEN_KEYS);

    if (vec.size() != 3) throw std::runtime_error{ERR_OPEN_KEYS};
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

  static constexpr const char* const ERR_CONSTRUCT_KEYS =
      "unable to construct metall_graph::keys object";
  static constexpr const char* const ERR_OPEN_KEYS =
      "unable to open metall_graph::keys object";

  static constexpr std::size_t NODE_KEY_IDX    = 0;
  static constexpr std::size_t EDGE_SRCKEY_IDX = NODE_KEY_IDX + 1;
  static constexpr std::size_t EDGE_TGTKEY_IDX = EDGE_SRCKEY_IDX + 1;
};

}  // namespace experimental
