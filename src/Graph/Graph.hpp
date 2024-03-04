#include <ygm/comm.hpp>
#include <stdexcept>
#include <cassert>

#include <metall/container/experimental/string_container/deque.hpp>

namespace metall_string_container =
    metall::container::experimental::string_container;
class Graph {
 public:
  template <typename Allocator, typename StringTable>
  Graph(Allocator allocator, StringTable st)
      : m_node_meta_name(st), m_node_meta_desc(st) {}

  bool add_meta(const std::string& name, const std::string& desc) {
    if (is_meta_node(name)) {
      int index = find_meta(name);
      if (index == -1) {  // name is new
        m_node_meta_name.push_back(name.c_str());
        m_node_meta_desc.push_back(desc.c_str());
        return true;
      } else {
        throw std::runtime_error("Duplicate metadata field");
      }
    } else if (is_meta_edge(name)) {
      throw std::runtime_error("EDGE: Not implemented yet");
    } else {
      throw std::runtime_error("UNKNOWN: Not implemented yet");
    }
  }

  int find_meta(const std::string& name) {
    if (is_meta_node(name)) {
      assert(m_node_meta_name.size() == m_node_meta_desc.size());
      for (size_t i = 0; i < m_node_meta_name.size(); ++i) {
        if (m_node_meta_name[i] == name.c_str()) {
          return i;
        }
      }
      return -1;  // not found
    } else if (is_meta_edge(name)) {
      throw std::runtime_error("EDGE: Not implemented yet");
    } else {
      throw std::runtime_error("UNKNOWN: Not implemented yet");
    }
  }

  std::map<std::string, std::string> get_meta_map() {
    std::map<std::string, std::string> to_return;

    for (size_t i = 0; i < m_node_meta_name.size(); ++i) {
      to_return[m_node_meta_name[i].c_str()] = m_node_meta_desc[i].c_str();
    }

    return to_return;
  }

 private:
  bool is_meta_node(const std::string& name) {
    return name.compare(0, 4, "node") == 0;
  }

  bool is_meta_edge(const std::string& name) {
    return name.compare(0, 4, "edge") == 0;
  }

  metall_string_container::deque<> m_node_meta_name;
  metall_string_container::deque<> m_node_meta_desc;
};

/////  Random Helpers //

std::string get_selector_name(boost::json::object&& jo) {
  std::string to_return;
  try {
    if (jo["expression_type"].as_string() != std::string("jsonlogic")) {
      std::cerr << " NOT A THINGY " << std::endl;
      exit(-1);
    }
    to_return = jo["rule"].as_object()["var"].as_string().c_str();
  } catch (...) {
    std::cerr << "!! ERROR !!" << std::endl;
    exit(-1);
  }
  return to_return;
}