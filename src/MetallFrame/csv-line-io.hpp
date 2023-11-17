#pragma once

#include <utility>
//~ #include <cassert>
//~ #include <stdexcept>
//~ #include <iostream>
#include <fstream>
#include <sstream>
#include <functional>

#include "experimental/cxx-compat.hpp"


namespace
{

void process_quoted_char(std::istream& stream, std::string& buf, bool inclQuotes)
{
  if (inclQuotes) buf.push_back('"');

  while (true)
  {
    int ch = stream.get();

    assert(ch != std::char_traits<char>::eof());
    if (ch == '"')
    {
      CXX_UNLIKELY;
      if (inclQuotes) buf.push_back('"');

      if (stream.peek() != '"')
        return;

      ch = stream.get();
    }

    buf.push_back(char(ch));
  }
}

auto read_str(std::istream& stream, char sep = ',', bool inclQuotes = false) -> std::string
{
  if (stream.eof()) return std::string{};

  std::string res;
  int         ch = stream.get();

  while ((ch != sep) && (ch != std::char_traits<char>::eof()))
  {
    if (ch == '"')
    {
      CXX_UNLIKELY;
      process_quoted_char(stream, res, inclQuotes);
    }
    else
      res.push_back(char(ch));

    ch = stream.get();
  }

  return res;
}

template <class ElemType>
auto read_tuple_variant( std::istream& stream,
                         const std::vector<std::function<ElemType(const std::string&)> >& adapt
                       ) -> std::vector<ElemType>
{
  std::vector<ElemType> res;

  for (const std::function<ElemType(const std::string&)>& celladapter : adapt)
    res.emplace_back(celladapter(read_str(stream)));

  return res;
}



} // namespace anonymous
