// Copyright Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

// PruneJuice Local Constraint Checking (LCC).
// Reza, Ripeanu, Tripoul, Sanders, Pearce. "PruneJuice: Pruning Trillion-edge
// Graphs to a Precise Pattern-Matching Solution", SC18 (Algs. 2-4).
//
// Every background vertex v keeps omega(v), the set of template vertices it
// may still match (a bitmask), and its active neighbors. Each round, active
// vertices send omega to their neighbors; then each vertex
//  - drops neighbors that did not report or that cannot match any template
//    neighbor of any candidate in omega(v) (edge elimination), and
//  - drops candidates q from omega(v) whose template neighbors cannot be
//    covered by distinct remaining neighbors (vertex refinement, checked
//    exactly as a small bipartite matching).
// Rounds repeat until nothing changes. All graphs are treated as undirected.

#include <algorithm>
#include <bit>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>
#include <ygm/comm.hpp>

#include <metalldata/metall_graph.hpp>

namespace metalldata {

namespace {

using node_locator = metall_graph::node_locator;
using edge_locator = metall_graph::edge_locator;

constexpr uint64_t bit(size_t i) { return uint64_t{1} << i; }

template <typename Fn>
void for_each_bit(uint64_t mask, Fn fn) {
  for (; mask != 0; mask &= mask - 1) {
    fn(static_cast<size_t>(std::countr_zero(mask)));
  }
}

struct lcc_nbr {
  node_locator nbr;
  uint64_t     emask = 0;  ///< template edges allowed by any parallel edge
  uint64_t     recv = 0;   ///< omega(nbr) received this round, then phi
  std::vector<std::pair<edge_locator, uint64_t>> edges;  ///< per-edge masks
};

struct lcc_vertex {
  uint64_t             omega = 0;
  bool                 active = false;
  std::vector<lcc_nbr> adj;  ///< sorted by nbr
};

/// Template adjacency: for each template vertex q, its (neighbor q', edge e).
using template_adj = std::vector<std::vector<std::pair<size_t, size_t>>>;

/// Template vertices q' that `nb` may match given our candidates `omega`.
uint64_t compute_phi(const template_adj& tadj, uint64_t omega,
                     const lcc_nbr& nb, uint64_t nb_omega) {
  uint64_t allowed = 0;
  for_each_bit(omega, [&](size_t q) {
    for (const auto& [qp, e] : tadj[q]) {
      if (nb.emask & bit(e)) {
        allowed |= bit(qp);
      }
    }
  });
  return allowed & nb_omega;
}

/// True if each of the k template neighbors can be assigned a distinct
/// background neighbor. groups holds (signature over k, count) pairs.
bool can_cover(size_t k, const std::vector<std::pair<uint64_t, size_t>>& groups) {
  std::vector<size_t>              used(groups.size(), 0);
  std::vector<std::vector<size_t>> assigned(groups.size());
  std::vector<char>                visited;

  std::function<bool(size_t)> augment = [&](size_t t) -> bool {
    for (size_t g = 0; g < groups.size(); ++g) {
      if (visited[g] || !(groups[g].first & bit(t))) {
        continue;
      }
      visited[g] = 1;
      if (used[g] < groups[g].second) {
        ++used[g];
        assigned[g].push_back(t);
        return true;
      }
      for (auto& other : assigned[g]) {
        if (augment(other)) {
          other = t;
          return true;
        }
      }
    }
    return false;
  };

  for (size_t t = 0; t < k; ++t) {
    visited.assign(groups.size(), 0);
    if (!augment(t)) {
      return false;
    }
  }
  return true;
}

/// Can template vertex q be matched at a vertex with neighbors `adj`, whose
/// recv fields hold phi?
bool meets_neighbor_constraints(const template_adj& tadj, size_t q,
                                const std::vector<lcc_nbr>& adj) {
  const auto& tnbrs = tadj[q];
  const size_t k = tnbrs.size();
  if (adj.size() < k) {
    return false;
  }

  boost::unordered_flat_map<uint64_t, size_t> sig_counts;
  uint64_t                                    covered = 0;
  for (const auto& nb : adj) {
    uint64_t sig = 0;
    for (size_t t = 0; t < k; ++t) {
      const auto& [qp, e] = tnbrs[t];
      if ((nb.emask & bit(e)) && (nb.recv & bit(qp))) {
        sig |= bit(t);
      }
    }
    if (sig != 0) {
      auto& c = sig_counts[sig];
      c = std::min(c + 1, k);
      covered |= sig;
    }
  }
  const uint64_t all = (k == 64) ? ~uint64_t{0} : bit(k) - 1;
  if (covered != all) {
    return false;
  }

  std::vector<std::pair<uint64_t, size_t>> groups(sig_counts.begin(),
                                                  sig_counts.end());
  return can_cover(k, groups);
}

}  // namespace

result<> metall_graph::pattern_template::validate() const {
  const size_t nq = vertex_names.size();
  if (nq == 0 || nq > max_vertices) {
    return std::unexpected(std::format(
      "template must have between 1 and {} vertices", max_vertices));
  }
  if (vertex_preds.size() != nq) {
    return std::unexpected("template vertex predicates mismatch");
  }
  if (edges.empty() || edges.size() > max_edges) {
    return std::unexpected(
      std::format("template must have between 1 and {} edges", max_edges));
  }

  auto check_pred = [](const where_clause& w) {
    return w.empty() || w.is_node_clause() || w.is_edge_clause();
  };
  for (size_t q = 0; q < nq; ++q) {
    if (!check_pred(vertex_preds[q])) {
      return std::unexpected(std::format(
        "template vertex {}: predicate must reference only node or only edge "
        "series",
        vertex_names[q]));
    }
  }

  std::vector<std::vector<size_t>> adj(nq);
  for (const auto& e : edges) {
    if (e.u >= nq || e.v >= nq) {
      return std::unexpected("template edge endpoint out of range");
    }
    if (e.u == e.v) {
      return std::unexpected(
        std::format("template self loop on {}", vertex_names[e.u]));
    }
    if (std::ranges::find(adj[e.u], e.v) != adj[e.u].end()) {
      return std::unexpected(std::format("duplicate template edge {} - {}",
                                         vertex_names[e.u], vertex_names[e.v]));
    }
    if (!check_pred(e.pred)) {
      return std::unexpected(std::format(
        "template edge {} - {}: predicate must reference only node or only "
        "edge series",
        vertex_names[e.u], vertex_names[e.v]));
    }
    adj[e.u].push_back(e.v);
    adj[e.v].push_back(e.u);
  }

  std::vector<char>   seen(nq, 0);
  std::vector<size_t> stack{0};
  seen[0] = 1;
  size_t nseen = 1;
  while (!stack.empty()) {
    auto q = stack.back();
    stack.pop_back();
    for (auto qp : adj[q]) {
      if (!seen[qp]) {
        seen[qp] = 1;
        ++nseen;
        stack.push_back(qp);
      }
    }
  }
  if (nseen != nq) {
    return std::unexpected("template must be connected");
  }
  return {};
}

result<metall_graph::lcc_stats> metall_graph::local_constraint_check(
  const series_name& out_name, const pattern_template& tmpl,
  const where_clause& where) {
  if (!out_name.is_edge_series()) {
    return std::unexpected(
      std::format("Invalid edge series name: {}", out_name.qualified()));
  }
  if (has_series(out_name)) {
    return std::unexpected(
      std::format("Series {} already exists", out_name.qualified()));
  }
  if (auto v = tmpl.validate(); !v) {
    return std::unexpected(v.error());
  }

  const size_t nq = tmpl.num_vertices();
  template_adj tadj(nq);
  for (size_t e = 0; e < tmpl.edges.size(); ++e) {
    tadj[tmpl.edges[e].u].emplace_back(tmpl.edges[e].v, e);
    tadj[tmpl.edges[e].v].emplace_back(tmpl.edges[e].u, e);
  }

  using state_map = boost::unordered_flat_map<local_node_idx_type, lcc_vertex>;
  state_map         state;
  static state_map* sp_state;
  sp_state = &state;

  //
  // Candidate initialization: omega(v) = template vertices whose predicate v
  // satisfies. Wildcard vertices are applied during the adjacency build.
  uint64_t wild_vmask = 0;
  for (size_t q = 0; q < nq; ++q) {
    if (tmpl.vertex_preds[q].empty()) {
      wild_vmask |= bit(q);
      continue;
    }
    priv_for_all_nodes(
      [&](local_node_idx_type nid) { state[nid].omega |= bit(q); },
      tmpl.vertex_preds[q]);
  }

  //
  // Template edges each local edge may match.
  boost::unordered_flat_map<local_edge_idx_type, uint64_t> edge_masks;
  uint64_t                                                 wild_emask = 0;
  for (size_t e = 0; e < tmpl.edges.size(); ++e) {
    if (tmpl.edges[e].pred.empty()) {
      wild_emask |= bit(e);
      continue;
    }
    priv_for_all_edges(
      [&](local_edge_idx_type eid) { edge_masks[eid] |= bit(e); },
      tmpl.edges[e].pred);
  }

  //
  // Build undirected adjacency on the owners of each endpoint.
  auto add_nbr = [](local_node_idx_type lid, node_locator nbr, edge_locator el,
                    uint64_t emask, uint64_t wild) {
    auto& st = (*sp_state)[lid];
    st.omega |= wild;
    st.adj.push_back(lcc_nbr{nbr, emask, 0, {{el, emask}}});
  };
  priv_for_all_edges(
    [&](local_edge_idx_type eid) {
      auto [u, v] = pl_get_edge_uv_locators(eid);
      if (u == v) {
        return;
      }
      uint64_t emask = wild_emask;
      if (auto it = edge_masks.find(eid); it != edge_masks.end()) {
        emask |= it->second;
      }
      if (emask == 0) {
        return;
      }
      auto el = make_edge_locator(m_comm.rank(), eid);
      m_comm.async(owner(u), add_nbr, local(u), v, el, emask, wild_vmask);
      m_comm.async(owner(v), add_nbr, local(v), u, el, emask, wild_vmask);
    },
    where);
  m_comm.barrier();
  edge_masks.clear();

  // Merge parallel edges into a single neighbor entry.
  for (auto& [lid, st] : state) {
    std::ranges::sort(st.adj, {}, &lcc_nbr::nbr);
    std::vector<lcc_nbr> merged;
    for (auto& nb : st.adj) {
      if (!merged.empty() && merged.back().nbr == nb.nbr) {
        merged.back().emask |= nb.emask;
        merged.back().edges.insert(merged.back().edges.end(), nb.edges.begin(),
                                   nb.edges.end());
      } else {
        merged.push_back(std::move(nb));
      }
    }
    st.adj = std::move(merged);
    st.active = st.omega != 0 && !st.adj.empty();
  }

  //
  // LCC rounds until fixpoint.
  auto recv_omega = [](local_node_idx_type lid, node_locator sender,
                       uint64_t omega) {
    auto it = sp_state->find(lid);
    if (it == sp_state->end() || !it->second.active) {
      return;
    }
    auto& adj = it->second.adj;
    auto  nit = std::ranges::lower_bound(adj, sender, {}, &lcc_nbr::nbr);
    if (nit != adj.end() && nit->nbr == sender) {
      nit->recv = omega;
    }
  };

  lcc_stats stats;
  bool      changed = true;
  while (changed) {
    ++stats.rounds;
    for (auto& [lid, st] : state) {
      for (auto& nb : st.adj) {
        nb.recv = 0;
      }
    }
    m_comm.barrier();

    for (const auto& [lid, st] : state) {
      if (!st.active) {
        continue;
      }
      auto self = make_node_locator(m_comm.rank(), lid);
      for (const auto& nb : st.adj) {
        m_comm.async(owner(nb.nbr), recv_omega, local(nb.nbr), self, st.omega);
      }
    }
    m_comm.barrier();

    bool local_changed = false;
    for (auto& [lid, st] : state) {
      if (!st.active) {
        continue;
      }
      const auto old_omega = st.omega;
      const auto old_degree = st.adj.size();
      // Edge elimination.
      std::erase_if(st.adj, [&](lcc_nbr& nb) {
        nb.recv = compute_phi(tadj, st.omega, nb, nb.recv);
        return nb.recv == 0;
      });
      // Vertex refinement.
      for_each_bit(st.omega, [&](size_t q) {
        if (!meets_neighbor_constraints(tadj, q, st.adj)) {
          st.omega &= ~bit(q);
        }
      });
      if (st.omega == 0 || st.adj.empty()) {
        st.active = false;
      }
      local_changed |= st.omega != old_omega || st.adj.size() != old_degree;
    }
    changed = ygm::logical_or(local_changed, m_comm);
  }

  //
  // Output: false for every in-scope edge, then true for survivors.
  size_t local_nodes = 0;
  size_t local_edges = 0;

  // Writable only while writing the output; the closing barrier must stay
  // inside.
  {
    write_guard wguard(*this);
    auto        out_idx = priv_add_edge_series<bool>(out_name.unqualified());
    priv_for_all_edges(
      [&](local_edge_idx_type eid) { pl_set_edge_field(out_idx, eid, false); },
      where);
    m_comm.barrier();

    for (const auto& [lid, st] : state) {
      if (!st.active) {
        continue;
      }
      ++local_nodes;
      auto self = make_node_locator(m_comm.rank(), lid);
      for (const auto& nb : st.adj) {
        if (!(self < nb.nbr)) {
          continue;
        }
        for (const auto& [el, emask] : nb.edges) {
          lcc_nbr single{nb.nbr, emask, 0, {}};
          if (compute_phi(tadj, st.omega, single, nb.recv) == 0) {
            continue;
          }
          ++local_edges;
          m_comm.async(
            owner(el),
            [](ygm_ptr_type p, local_edge_idx_type eid,
               edge_series_idx_type sidx) {
              p->pl_set_edge_field(sidx, eid, true);
            },
            pthis, local(el), out_idx);
        }
      }
    }
    m_comm.barrier();
  }

  stats.num_nodes = ygm::sum(local_nodes, m_comm);
  stats.num_edges = ygm::sum(local_edges, m_comm);
  sp_state = nullptr;
  return stats;
}

}  // namespace metalldata
