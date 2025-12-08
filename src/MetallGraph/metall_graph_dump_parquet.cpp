#include <metall_graph.hpp>
#include <parquet_writer/parquet_writer.hpp>
#include <format>
#include <fstream>

namespace metalldata {
metall_graph::return_code metall_graph::dump_parquet_verts(
  std::string_view path, const std::vector<series_name>& meta, bool overwrite) {
  return_code to_return;

  // Build field specifications: node.id (string) + metadata columns
  std::vector<std::string> field_specs;
  field_specs.reserve(1 + meta.size());

  // Add the node ID column (always a string)
  field_specs.push_back(std::format("{}:s", NODE_COL.qualified()));

  // Add metadata columns with their types
  // Collect series indices first
  std::vector<std::pair<size_t, series_name>> meta_series;  // index, name

  for (const auto& sn : meta) {
    if (!has_series(sn)) {
      to_return
        .warnings[std::format("Column '{}' not found", sn.qualified())]++;
      continue;
    }
    auto idx = m_pnodes->find_series(sn.unqualified());
    meta_series.emplace_back(idx, sn);
  }

  // Determine types by sampling rows until we get real types for all columns
  std::vector<std::pair<size_t, char>> meta_info;  // index, type_char
  meta_info.resize(meta_series.size());
  std::vector<bool> type_determined(meta_series.size(), false);

  for (record_id_type rid = 0; rid < m_pnodes->num_records(); ++rid) {
    if (!m_pnodes->contains_record(rid)) continue;

    bool all_determined = true;
    for (size_t i = 0; i < meta_series.size(); ++i) {
      if (type_determined[i]) continue;

      auto [idx, col_name] = meta_series[i];
      auto sample_val      = m_pnodes->get_dynamic(idx, rid);

      char type_char  = 's';  // default to string
      bool found_type = false;
      std::visit(
        [&type_char, &found_type](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            found_type = false;
          } else if constexpr (std::is_same_v<T, bool>) {
            type_char  = 'b';
            found_type = true;
          } else if constexpr (std::is_same_v<T, int64_t>) {
            type_char  = 'i';
            found_type = true;
          } else if constexpr (std::is_same_v<T, uint64_t>) {
            type_char  = 'u';
            found_type = true;
          } else if constexpr (std::is_same_v<T, double>) {
            type_char  = 'f';
            found_type = true;
          } else if constexpr (std::is_same_v<T, std::string_view>) {
            type_char  = 's';
            found_type = true;
          }
        },
        sample_val);

      if (found_type) {
        meta_info[i]       = {idx, type_char};
        type_determined[i] = true;
      } else {
        all_determined = false;
      }
    }

    if (all_determined) break;
  }

  m_comm.cerr0("meta_series.size = ", meta_series.size());
  // Build field specs with determined types
  for (size_t i = 0; i < meta_series.size(); ++i) {
    auto [idx, sn] = meta_series[i];
    char type_char = type_determined[i] ? meta_info[i].second : 's';
    field_specs.push_back(std::format("{}:{}", sn.unqualified(), type_char));
    if (!type_determined[i]) {
      meta_info[i] = {idx, 's'};  // default to string if never determined
    }
  }

  // Create filename based on rank
  std::string filename =
    std::format("{}_rank{}.parquet", std::string(path), m_comm.rank());

  // Check if file exists and handle overwrite flag
  if (!overwrite) {
    std::ifstream file_check(filename);
    if (file_check.good()) {
      to_return.error = std::format(
        "File '{}' already exists and overwrite is false", filename);
      return to_return;
    }
  }

  try {
    // Create ParquetWriter
    parquet_writer::ParquetWriter writer(filename, field_specs);

    if (!writer.is_valid()) {
      to_return.error = "Failed to create Parquet writer";
      return to_return;
    }

    // Prepare node ID series index
    auto node_col_idx = m_pnodes->find_series(NODE_COL.unqualified());

    // Write rows
    m_pnodes->for_all_rows([&](record_id_type rid) {
      std::vector<parquet_writer::metall_series_type> row;
      row.reserve(1 + meta_info.size());

      // Add node ID
      auto node_val = m_pnodes->get_dynamic(node_col_idx, rid);
      std::visit(
        [&row](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::string_view>) {
            row.emplace_back(v);
          } else {
            // Node ID should always be a string, but handle other cases
            row.emplace_back(v);
          }
        },
        node_val);

      // Add metadata columns
      for (const auto& [idx, type_char] : meta_info) {
        auto val = m_pnodes->get_dynamic(idx, rid);

        // Convert to parquet_writer type
        std::visit(
          [&row](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
              row.emplace_back(std::monostate{});
            } else if constexpr (std::is_same_v<T, bool>) {
              row.emplace_back(v);
            } else if constexpr (std::is_same_v<T, int64_t>) {
              row.emplace_back(v);
            } else if constexpr (std::is_same_v<T, uint64_t>) {
              row.emplace_back(v);
            } else if constexpr (std::is_same_v<T, double>) {
              row.emplace_back(v);
            } else if constexpr (std::is_same_v<T, std::string_view>) {
              row.emplace_back(v);
            }
          },
          val);
      }

      auto status = writer.write_row(row);
      if (!status.ok()) {
        to_return.warnings["Write errors"]++;
      }
    });

    // Flush and close
    auto flush_status = writer.flush();
    if (!flush_status.ok()) {
      to_return.warnings["Flush failed"]++;
    }

    auto close_status = writer.close();
    if (!close_status.ok()) {
      to_return.warnings["Close failed"]++;
    }

    to_return.return_info["rows_written"] = m_pnodes->num_records();
    to_return.return_info["filename"]     = filename;

  } catch (const std::exception& e) {
    to_return.error = std::format("Exception: {}", e.what());
  }

  return to_return;
}
}  // namespace metalldata
