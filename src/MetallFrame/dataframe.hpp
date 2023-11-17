// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief This file contains common code for the dataframe implementation

#pragma once

#include <cassert>
#include <utility>
#include <memory>
#include <variant>
#include <string>

// #include <span>
//~ #include <map>

#include <boost/container/vector.hpp>
#include <boost/container/string.hpp>

#include <metall/metall.hpp>
#include <metall/tags.hpp>

#include <experimental/flat_map.hpp>
#include <experimental/cxx-compat.hpp>

namespace experimental
{

//~ using string_t = std::string;
//~ using string_t  = boost::container::string;
using string_t            = boost::container::basic_string< char,
                                                            std::char_traits<char>,
                                                            metall::manager::allocator_type<char>
                                                          >;

using int_t               = int64_t;
using uint_t              = uint64_t;
using real_t              = double;
struct notavail_t{};

using dataframe_variant_t = std::variant<notavail_t, int_t, real_t, uint_t, string_t>;

struct runtime_type_error : std::runtime_error
{
  using base = std::runtime_error;
  using base::base;
};

struct unknown_column_error : std::runtime_error
{
  using base = std::runtime_error;
  using base::base;
};


template <class T>
struct default_value
{
    default_value(default_value&&)                 = default;
    default_value(const default_value&)            = default;
    default_value& operator=(default_value&&)      = default;
    default_value& operator=(const default_value&) = default;
    ~default_value()                              = default;

    explicit
    default_value(T el)
    : defaultval(std::move(el))
    {}

    const T& value() const
    {
      return defaultval;
    }

  private:
    T defaultval;

    default_value() = delete;
};

template < class KeyT,
           class ElemT,
           class Compare = std::less<KeyT>,
           class Alloc = metall::manager::allocator_type<std::pair<KeyT, ElemT> >,
           template <class, class> class Vector = boost::container::vector
         >
struct sparse_column : default_value<ElemT>, flat_map<KeyT, ElemT, Compare, Alloc, Vector>
{
  using defvalbase     = default_value<ElemT>;
  using base           = flat_map<KeyT, ElemT, Compare, Alloc, Vector>;
  using key_compare    = typename base::key_compare;
  using allocator_type = typename base::allocator_type;
  using value_type     = typename base::value_type;
  using mapped_type    = typename base::mapped_type;

/*
  explicit
  sparse_column(const key_compare& keycomp, const allocator_type& alloc = allocator_type())
  : base(alloc, keycomp), defvalbase()
  {}
*/

  sparse_column(const allocator_type& alloc, mapped_type defval, value_type prototype)
  : defvalbase(std::move(defval)), base(alloc, std::move(prototype))
  {}
};



template < class ValT,
           class AllocT = metall::manager::allocator_type<ValT>,
           template <class, class> class Vector = boost::container::vector
         >
struct dense_column : default_value<ValT>, Vector<ValT, AllocT>
{
  using defvalbase     = default_value<ValT>;
  using base           = Vector<ValT, AllocT>;
  using allocator_type = typename base::allocator_type;
  using value_type     = typename base::value_type;

  explicit
  dense_column(const allocator_type& alloc, value_type defval = value_type{}, size_t rows = 0)
  : defvalbase(std::move(defval)), base(rows, defvalbase::value(), alloc)
  {}

  void resize(size_t sz)
  {
    base::resize(sz, defvalbase::default_value());
  }
};

template <class T>
using dense_vector_t  = dense_column<T>;
//~ using dense_vector_t  = boost::container::vector<T, metall::manager::allocator_type<T>>;
//~ using dense_vector_t  = dense_column<T, metall::manager::allocator_type<T>>;


template <class T>
using sparse_vector_t = sparse_column<size_t, T>;

namespace
{

[[noreturn]]
inline
void error_type_mismatch(std::string_view cell = {}, std::string_view xpct = {})
{
  std::string err{"type mismatch:"};

  if (cell.size()) (err += " got ") += cell;
  if (xpct.size()) (err += " expected ") += xpct;

  throw runtime_type_error(err);
}

template <class T>
constexpr
const T* tag() { return nullptr; }

} // anonymous namespace

template <class ElemType>
struct abstract_column_iterator
{
  using value_type = ElemType;

  virtual ~abstract_column_iterator() {}
  virtual value_type& deref() = 0;
  virtual size_t row() const = 0;
  virtual void next() = 0;
  virtual void prev() = 0;
  virtual bool equals(const abstract_column_iterator& other) const = 0;
  virtual abstract_column_iterator* clone() const = 0;
};

namespace
{

template <class T>
struct dense_column_iterator : abstract_column_iterator<T>
{
    using base       = abstract_column_iterator<T>;
    using this_type   = dense_column_iterator<T>;
    using value_type = typename base::value_type;
    using VectorRep  = dense_vector_t<T>;
    using Iterator   = typename VectorRep::iterator;

    dense_column_iterator(Iterator pos, size_t rowcnt)
    : base(), it(pos), rownum(rowcnt)
    {}

    value_type& deref()       override { return *(this->it); }
    size_t      row()   const override { return this->rownum; }
    void        next()        override { ++(this->it); ++(this->rownum); }
    void        prev()        override { --(this->it); --(this->rownum); }

    bool equals(const base& other) const override
    {
      assert(typeid(*this) == typeid(other));

      const this_type& rhs = static_cast<const this_type&>(other);

      return this->it == rhs.it; // equal rownum is implied by equal iterators
    }

    this_type* clone() const override
    {
      return new this_type(it, rownum);
    }

  private:
    Iterator it;
    size_t              rownum;
};

template <class T>
struct sparse_column_iterator : abstract_column_iterator<T>
{
    using base       = abstract_column_iterator<T>;
    using this_type   = sparse_column_iterator<T>;
    using value_type = typename base::value_type;
    using VectorRep  = sparse_vector_t<T>;
    using Iterator   = typename VectorRep::iterator;

    sparse_column_iterator(Iterator pos)
    : base(), it(pos)
    {}

    value_type& deref()     override { return this->it->second; }
    size_t      row() const override { return this->it->first; }
    void        next()      override { ++(this->it); }
    void        prev()      override { --(this->it); }

    bool equals(const base& other) const override
    {
      assert(typeid(*this) == typeid(other));

      const this_type& rhs = static_cast<const this_type&>(other);

      return this->it == rhs.it; // equal rownum is implied by equal iterators
    }

    this_type* clone() const override
    {
      return new this_type(it);
    }

  private:
    Iterator it;
};

const std::string string_type_str{"string_t"};
const std::string int_type_str{"int_t"};
const std::string uint_type_str{"uint_t"};
const std::string real_type_str{"real_t"};

inline std::string to_string(const string_t*) { return string_type_str; }
inline std::string to_string(const int_t*)    { return int_type_str; }
inline std::string to_string(const uint_t*)   { return uint_type_str; }
inline std::string to_string(const real_t*)   { return real_type_str; }

template <class T>
inline std::string to_string(const T*)
{
  return std::string{"unknown type; mangled name is: "} + typeid(T).name();
}

} // anonymous namespace

template <class T>
struct any_column_iterator
{
  using iterator_category = std::bidirectional_iterator_tag;
  using value_type = T;
  using difference_type = int;
  using pointer = value_type*;
  using reference = value_type&;
  using this_type = any_column_iterator<T>;

  explicit
  any_column_iterator(abstract_column_iterator<T>* obj)
  : pit(obj)
  {}

  explicit
  any_column_iterator(typename dense_vector_t<T>::iterator it, size_t pos)
  : any_column_iterator(new dense_column_iterator<T>(it, pos))
  {}

  explicit
  any_column_iterator(typename sparse_vector_t<T>::iterator it)
  : any_column_iterator(new sparse_column_iterator<T>(it))
  {}

  any_column_iterator(const any_column_iterator& other)
  : any_column_iterator(other.pit->clone())
  {}

  any_column_iterator(any_column_iterator&& other)
  : any_column_iterator(other.pit)
  {
    other.pit = nullptr;
  }

  any_column_iterator& operator=(const any_column_iterator& other)
  {
    any_column_iterator tmp(other.pit->clone());

    std::swap(tmp.pit, this->pit);
    return *this;
  }

  any_column_iterator& operator=(any_column_iterator&& other)
  {
    std::swap(other.pit, this->pit);
    return *this;
  }

  ~any_column_iterator()
  {
    delete pit;
  }

  T& operator*() { return pit->deref(); }
  size_t row() const { return pit->row(); }
  this_type& operator++() { pit->next(); return *this; }

  this_type operator++(int)
  {
    this_type res{pit->clone()};

    pit->next();
    return *this;
  }

  this_type& operator--() { pit->prev(); return *this; }

  this_type operator--(int)
  {
    this_type res{pit->clone()};

    pit->prev();
    return *this;
  }

  bool operator==(any_column_iterator& that) const
  {
    if (pit == nullptr || that.pit == nullptr)
      return pit == that.pit;

    return pit->equals(*(that.pit));
  }

  bool operator!=(any_column_iterator& that) const
  {
    return !(*this == that);
  }

  private:
    abstract_column_iterator<T>* pit;
};


namespace
{
/// interface for diagnostics
struct vector_accessor_runtime_info
{
  /// returns the type of the element
  virtual std::string type_name() const = 0;
};

/// define operations on columns.
template <class T>
struct vector_accessor_base_generic : virtual vector_accessor_runtime_info
{
  using entry_type = std::pair<const size_t, T>;

  virtual
  dense_vector_t<T>& data(void* /*cont*/, const dense_vector_t<T>* /*tag*/) const
  {
    error_type_mismatch(type_name(), to_string(tag<T>()));
  }

  virtual
  sparse_vector_t<T>& data(void* /*cont*/, const sparse_vector_t<T>* /*tag*/) const
  {
    error_type_mismatch(type_name(), to_string(tag<T>()));
  }

  virtual
  T* at(void* /*cont*/, size_t /*pos*/, const T* /*tag*/) const
  {
    error_type_mismatch(type_name(), to_string(tag<T>()));
  }

  virtual
  const T& default_value(void* /*cont*/, const T* /*tag*/) const
  {
    error_type_mismatch(type_name(), to_string(tag<T>()));
  }

  virtual
  T& cell(void* /*cont*/, size_t /*pos*/, const T* /*tag*/) const
  {
    error_type_mismatch(type_name(), to_string(tag<T>()));
  }

  virtual
  void add(void* /*cont*/, T&&) const
  {
    error_type_mismatch(type_name(), to_string(tag<T>()));
  }

  virtual
  std::pair<any_column_iterator<T>, any_column_iterator<T> >
  range(void* /*cont*/, const T*) const
  {
    error_type_mismatch(type_name(), typeid(T).name());
  }
};


// define the column types
struct vector_accessor_base : vector_accessor_base_generic<string_t>
                            , vector_accessor_base_generic<int_t>
                            , vector_accessor_base_generic<uint_t>
                            , vector_accessor_base_generic<real_t>
{
  //
  // variant based types
  using value_variant_t           = dataframe_variant_t;
  using pointer_variant_t         = std::variant<int_t*, uint_t*, real_t*, string_t*>;
  //~ using const_reference_variant_t = std::variant<const int_t&, const uint_t&, const real_t&, const string_t&>;
  using iterator_variant_t        = std::variant< dense_vector_t<int_t>::iterator,    sparse_vector_t<int_t>::iterator
                                                , dense_vector_t<uint_t>::iterator,   sparse_vector_t<uint_t>::iterator
                                                , dense_vector_t<real_t>::iterator,   sparse_vector_t<real_t>::iterator
                                                , dense_vector_t<string_t>::iterator, sparse_vector_t<string_t>::iterator
                                                >;
  using range_variant_t           = std::pair<iterator_variant_t, iterator_variant_t>;

  //
  // element-type independent API

  /// returns true iff this is a sparse column
  virtual bool is_sparse() const = 0;

  /// writes back any data held in volatile memory
  virtual void persist(void* /*cont*/) const = 0;

  //
  // variant-based abstract API

  /// returns a pointer to an element at \ref pos, or nullptr if the value does not exist
  virtual pointer_variant_t at_variant(void* cont, size_t pos) const = 0;

  /// returns a reference to variant with a reference to a default value
  virtual value_variant_t default_value_variant(void* /*cont*/) const = 0;
  //~ virtual const_reference_variant_t default_value_variant(void* /*cont*/) const = 0;

  /// returns a variant to a value at \ref pos
  virtual value_variant_t cell_variant(void* /*cont*/, size_t pos) const = 0;

  /// adds a new element at the end of the column
  virtual void add_variant(void* /*cont*/, value_variant_t&& /* value */) const = 0;

  /// returns an iterator pair of the column
  virtual range_variant_t range_variant(void* /*cont*/) const = 0;

  ///
  virtual void clear(void*) const = 0;
};


template <template <class> class VectorT, class T>
struct vector_accessor_common : vector_accessor_base
{
  using VectorRep = VectorT<T>;

  VectorRep&
  data(void* vec, const VectorRep* /*tag*/) const override
  {
    VectorRep* container = static_cast<VectorRep*>(vec);

    assert(container);
    return *container;
  }

  bool is_sparse() const override
  {
    return std::is_same<VectorRep, sparse_vector_t<T> >::value;
  }

  std::string type_name() const override
  {
    return to_string(tag<T>());
  }

  VectorRep&
  data(void* vec) const
  {
    return data(vec, tag<VectorRep>());
  }

  const T&
  default_value(void* vec, const T* /*tag*/) const override
  {
    return data(vec).value();
  }

  value_variant_t
  cell_variant(void* vec, size_t row) const override
  {
    const vector_accessor_base_generic<T>& self = *this;
    T*                            res = self.at(vec, row, tag<T>());

    return res ? value_variant_t{*res} : value_variant_t{self.default_value(vec, tag<T>())};
  }

  void
  add_variant(void* vec, value_variant_t&& elem) const override
  {
    const vector_accessor_base_generic<T>& self = *this;

    self.add(vec, std::get<T>(std::move(elem)));
  }

  pointer_variant_t
  at_variant(void* cont, size_t pos) const override
  {
    const vector_accessor_base_generic<T>& self = *this;

    return pointer_variant_t{ self.at(cont, pos, tag<T>()) };
  }

  value_variant_t
  default_value_variant(void* vec) const override
  {
    return value_variant_t{ default_value(vec, tag<T>()) };
  }

  void clear(void* vec) const override
  {
    data(vec).clear();
  }
};

// dense vector accessor
template <class T>
struct dense_vector_accessor : vector_accessor_common<dense_vector_t, T>
{
  using base = vector_accessor_common<dense_vector_t, T>;
  using typename base::VectorRep;
  using typename base::range_variant_t;
  using typename base::iterator_variant_t;

  T*
  at(void* vec, size_t pos, const T*) const override
  {
    return &base::data(vec).at(pos);
  }

  T&
  cell(void* vec, size_t pos, const T*) const override
  {
    return base::data(vec).at(pos);
  }

  void
  add(void* vec, T&& el) const override
  {
    VectorRep& cont = base::data(vec);

    cont.emplace_back(std::move(el));
  }

  std::pair<any_column_iterator<T>, any_column_iterator<T> >
  range(void* cont, const T*) const override
  {
    using CommonIterator = any_column_iterator<T>;

    VectorRep& col = base::data(cont);

    return std::make_pair( CommonIterator{col.begin(), 0},
                           CommonIterator{col.end(), col.size()}
                         );
  }

  range_variant_t
  range_variant(void* cont) const override
  {
    VectorRep& col = base::data(cont);

    return range_variant_t{ iterator_variant_t{col.begin()}, iterator_variant_t{col.end()} };
  }

  void persist(void*) const override { /* nothing to do for dense vectors */ }
};

// sparse vector accessor
template <class T>
struct sparse_vector_accessor : vector_accessor_common<sparse_vector_t, T>
{
  using base = vector_accessor_common<sparse_vector_t, T>;
  using typename base::VectorRep;
  using typename base::range_variant_t;
  using typename base::iterator_variant_t;

  T*
  at(void* vec, size_t row, const T*) const override
  {
    using VecIterator = typename VectorRep::iterator;

    VectorRep&  cont = base::data(vec);
    VecIterator pos  = cont.find(row);

    if (pos == cont.find_end())
    {
      CXX_UNLIKELY;
      return nullptr;
    }

    return &pos->second;
  }

  T&
  cell(void* vec, size_t row, const T*) const override
  {
    return base::data(vec)[row];
  }

  void
  add(void* vec, T&& el) const override
  {
    VectorRep& cont = base::data(vec);

    cont.emplace(cont.size(), std::move(el));
  }

  std::pair<any_column_iterator<T>, any_column_iterator<T> >
  range(void* cont, const T*) const override
  {
    using CommonIterator = any_column_iterator<T>;

    VectorRep& col = base::data(cont);

    return std::make_pair( CommonIterator{col.begin()},
                           CommonIterator{col.end()}
                         );
  }

  range_variant_t
  range_variant(void* cont) const override
  {
    VectorRep& col = base::data(cont);

    return range_variant_t{ iterator_variant_t{col.begin()}, iterator_variant_t{col.end()} };
  }

  void persist(void* cont) const override
  {
    base::data(cont).persist();
  }
};


/// Value Wrapper for dense and sparse vectors
struct vector_accessor_any
{
    using value_variant_t    = vector_accessor_base::value_variant_t;
    using pointer_variant_t  = vector_accessor_base::pointer_variant_t;
    using iterator_variant_t = vector_accessor_base::iterator_variant_t;
    using range_variant_t    = vector_accessor_base::range_variant_t;

    template <class T>
    explicit
    vector_accessor_any(const dense_vector_t<T>* /* tag */)
    : v(new dense_vector_accessor<T>)
    {}

    template <class T>
    explicit
    vector_accessor_any(const sparse_vector_t<T>* /* tag */)
    : v(new sparse_vector_accessor<T>)
    {}

    ~vector_accessor_any() = default;


  private:
    template <class T>
    vector_accessor_base_generic<T>& vectorT() const
    {
      vector_accessor_base_generic<T>* obj = &*v;

      assert(obj);
      return *obj;
    }

    vector_accessor_base& vector() const
    {
      vector_accessor_base* obj = &*v;

      assert(obj);
      return *obj;
    }

  public:

    //
    // template based interface

    template <class T>
    T* at(void* cont, size_t row) const
    {
      return vectorT<T>().at(cont, row, tag<T>());
    }

    template <class T>
    const T& default_value(void* cont) const
    {
      return vectorT<T>().default_value(cont, tag<T>());
    }

    template <class T>
    T& cell(void* cont, size_t row) const
    {
      return vectorT<T>().cell(cont, row, tag<T>());
    }

    template <class T>
    void add(void* cont, T&& el) const
    {
      vectorT<T>().add(cont, std::move(el));
    }

    template <class ColType, class T = typename ColType::value_type>
    ColType&
    data(void* cont) const
    {
      return vectorT<T>().data(cont, tag<ColType>());
    }

    template <class T>
    std::pair<any_column_iterator<T>, any_column_iterator<T> >
    range(void* cont) const
    {
      return vectorT<T>().range(cont, tag<T>());
    }


    //
    // variant based interface

    value_variant_t
    cell_variant(void* cont, size_t pos) const
    {
      return vector().cell_variant(cont, pos);
    }

    void
    add_variant(void* cont, value_variant_t&& el) const
    {
      vector().add_variant(cont, std::move(el));
    }

    pointer_variant_t
    at_variant(void* cont, size_t pos) const
    {
      return vector().at_variant(cont, pos);
    }

    value_variant_t
    default_value_variant(void* cont) const
    {
      return vector().default_value_variant(cont);
    }

    range_variant_t
    range_variant(void* cont) const
    {
      return vector().range_variant(cont);
    }

    void clear(void* cont) const
    {
      return vector().clear(cont);
    }

    //
    // meta info

    std::string
    type_name() const
    {
      return vector().type_name();
    }

    bool
    is_sparse() const
    {
      return vector().is_sparse();
    }

    //
    // NVM data persistance

    void persist(void* cont)
    {
      vector().persist(cont);
    }

  private:
    vector_accessor_any()                                    = delete;
    vector_accessor_any(const vector_accessor_any&)            = delete;
    vector_accessor_any(vector_accessor_any&&)                 = delete;
    vector_accessor_any& operator=(vector_accessor_any&&)      = delete;
    vector_accessor_any& operator=(const vector_accessor_any&) = delete;

    const std::unique_ptr<vector_accessor_base> v;
};

struct column_variant
{
    using value_variant_t    = vector_accessor_base::value_variant_t;
    using pointer_variant_t  = vector_accessor_base::pointer_variant_t;
    using iterator_variant_t = vector_accessor_base::iterator_variant_t;
    using range_variant_t    = vector_accessor_base::range_variant_t;

    column_variant(const vector_accessor_any& acc, void* cont)
    : accessor(&acc), container(cont)
    {}

    ~column_variant() = default;

    value_variant_t
    cell_variant(size_t pos) const
    {
      return accessor->cell_variant(container, pos);
    }

    void
    add_variant(value_variant_t&& el) const
    {
      accessor->add_variant(container, std::move(el));
    }

    pointer_variant_t
    at_variant(size_t pos) const
    {
      return accessor->at_variant(container, pos);
    }

    value_variant_t
    default_value_variant() const
    {
      return accessor->default_value_variant(container);
    }

    range_variant_t
    range_variant() const
    {
      return accessor->range_variant(container);
    }

    void clear() const
    {
      accessor->clear(container);
    }

    //
    // meta info

    std::string
    type_name() const
    {
      return accessor->type_name();
    }

    bool
    is_sparse() const
    {
      return accessor->is_sparse();
    }

  private:
    const vector_accessor_any* accessor;
    void*                    container;

    column_variant() = delete;
};


static vector_accessor_any accessors[] =
           {
             vector_accessor_any{tag<dense_vector_accessor<string_t>::VectorRep>()},
             vector_accessor_any{tag<dense_vector_accessor<int_t>::VectorRep>()},
             vector_accessor_any{tag<dense_vector_accessor<uint_t>::VectorRep>()},
             vector_accessor_any{tag<dense_vector_accessor<real_t>::VectorRep>()},
             vector_accessor_any{tag<sparse_vector_accessor<string_t>::VectorRep>()},
             vector_accessor_any{tag<sparse_vector_accessor<int_t>::VectorRep>()},
             vector_accessor_any{tag<sparse_vector_accessor<uint_t>::VectorRep>()},
             vector_accessor_any{tag<sparse_vector_accessor<real_t>::VectorRep>()},
           };


template <class ColType>
struct column_traits
{};

template <size_t kind, class T>
struct column_traits_impl
{
  using type = T;

  enum { col = kind };
};


template <>
struct column_traits< dense_vector_t<string_t> >
: column_traits_impl<0, string_t> {};

template <>
struct column_traits< dense_vector_t<int_t> >
: column_traits_impl<1, int_t> {};

template <>
struct column_traits< dense_vector_t<uint_t> >
: column_traits_impl<2, uint_t> {};

template <>
struct column_traits< dense_vector_t<real_t> >
: column_traits_impl<3, real_t> {};

template <>
struct column_traits< sparse_vector_t<string_t> >
: column_traits_impl<4, string_t> {};

template <>
struct column_traits< sparse_vector_t<int_t> >
: column_traits_impl<5, int_t> {};

template <>
struct column_traits< sparse_vector_t<uint_t> >
: column_traits_impl<6, uint_t> {};

template <>
struct column_traits< sparse_vector_t<real_t> >
: column_traits_impl<7, real_t> {};

} // anonymous


template <class T>
struct cell_descriptor
{
    using type_name = T;

    cell_descriptor() : default_value() {}

    explicit
    cell_descriptor(const T& el) : default_value(el) {}

    explicit
    cell_descriptor(T&& el) : default_value(std::move(el)) {}

    cell_descriptor(const cell_descriptor&)            = default;
    cell_descriptor(cell_descriptor&&)                 = default;
    cell_descriptor& operator=(const cell_descriptor&) = default;
    cell_descriptor& operator=(cell_descriptor&&)      = default;
    ~cell_descriptor()                                 = default;

    T value() && { return std::move(default_value); }

  private:

    T default_value;
};

template <class T>
struct sparse : cell_descriptor<T>
{
  using base = cell_descriptor<T>;
  using base::base;
};

template <class T>
struct dense : cell_descriptor<T>
{
  using base = cell_descriptor<T>;
  using base::base;
};

struct column_desc
{
  std::string column_type;
  bool        is_sparse_column;

  template <class ColumnType>
  bool is() const { return column_type == to_string(tag<ColumnType>()); }
};

//~ template <class Key>
struct dataframe
{
    dataframe(metall::create_only_t, metall::manager& mgr, std::string_view dataframekey)
    : memmgr(mgr), key(dataframekey)
    {
      const char*       cKey        = key.c_str();
      const std::string colNameKey  = key + colnamesSuffix;
      const char*       colNameCKey = colNameKey.c_str();
      const std::string numRowKey   = key + numrowsSuffix;
      const char*       numRowCKey  = numRowKey.c_str();

      allColumns  = memmgr.construct<dense_vector_t<ColumnOfsRep> >(cKey)(memmgr.get_allocator());
      allColNames = memmgr.construct<ColumnNames>(colNameCKey)( memmgr.get_allocator(),
                                                                ColumnNames::value_type{persistent_string(), 0}
                                                              );
      numRows     = memmgr.construct<size_t>(numRowCKey)(0);
    }

    dataframe(metall::open_only_t, metall::manager& mgr, std::string_view dataframekey)
    : memmgr(mgr), key(dataframekey)
    {
      allColumns  = memmgr.find<dense_vector_t<ColumnOfsRep>>(key.c_str()).first;
      allColNames = memmgr.find<ColumnNames>((key + colnamesSuffix).c_str()).first;
      numRows     = memmgr.find<size_t>((key + numrowsSuffix).c_str()).first;
    }

    bool valid() const
    {
      return (allColumns != nullptr) & (allColNames != nullptr) & (numRows != nullptr);
    }

    ~dataframe()
    {
      persist();
    }

    size_t rows() const
    {
      return *numRows;
    }

    size_t columns() const
    {
      return allColumns->size();
    }

    metall::manager::allocator_type<char>
    string_allocator() const
    {
      return memmgr.get_allocator();
    }

    string_t
    persistent_string(std::string_view str) const
    {
      return string_t{str.data(), str.size(), string_allocator()};
    }

    string_t
    persistent_string(const char* str = "") const
    {
      return string_t{str, string_allocator()};
    }

    template <class... RowType>
    std::tuple<RowType...>
    get_row(int row, const std::tuple<RowType...>*) const
    {
      return get_row<RowType...>(row, std::make_index_sequence<sizeof... (RowType)>());
    }

    template <class... RowType>
    std::tuple<RowType...>
    get_row(int row, const std::tuple<RowType...>*, const std::vector<int>& idxlst) const
    {
      return get_row_idxlst<RowType...>(row, idxlst, std::make_index_sequence<sizeof... (RowType)>());
    }

/*
    std::vector<dataframe_variant_t>
    get_row_variant(int row) const
    {
      std::vector<dataframe_variant_t> res;

      for (auto col : idxlst)
        res.emplace_back(col >= 0 ? get_cell_variant(row, col) : dataframe_variant_t{});

      return res;
    }
*/
    std::vector<dataframe_variant_t>
    get_row_variant(int row, const std::vector<int>& idxlst) const
    {
      std::vector<dataframe_variant_t> res;

      for (auto col : idxlst)
        res.emplace_back(col >= 0 ? get_cell_variant(row, col) : dataframe_variant_t{});

      return res;
    }


    /// returns the index list of columns in \ref colnames
    /// \param  colnames  sequence of persistent strings referring to column-names
    /// \return sequence of indices of column names in \ref colnames
    std::vector<int>
    get_index_list(const std::vector<string_t>& colnames) const
    {
      std::vector<int> res;

      for (const string_t& colname : colnames)
        res.push_back(column_index(colname));

      return res;
    }

    /// returns the index list of columns in \ref colnames
    /// \tparam StringT a convertible to std::string_view (e.g., std::string)
    /// \param  colnames  sequence of column-names
    /// \return sequence of indices of column names in \ref colnames
    template <class StringT>
    std::vector<int>
    get_index_list_std(const std::vector<StringT>& colnames) const
    {
      std::vector<int> res;

      for (const StringT& colname : colnames)
      {
        try
        {
          res.push_back(column_index(colname));
        }
        catch (const unknown_column_error&)
        {
          res.push_back(-1);
        }
      }

      return res;
    }

    template <class ColType>
    void set_cell(int row, int col, ColType&& el)
    {
      ColumnRep rep = column_at(col);

      accessors[rep.first].cell<ColType>(rep.second, row) = std::move(el);
    }

    // adds a new row
    template <class... RowType>
    void add(std::tuple<RowType...>&& el)
    {
      ++*numRows;
      return add_row<RowType...>(std::move(el), std::make_index_sequence<sizeof... (RowType)>());
    }

    void add(std::vector<dataframe_variant_t>&& row)
    {
      std::size_t col = 0;

      ++*numRows;

      for (dataframe_variant_t& cell : row)
        add_col_variant(std::move(cell), col++);
    }

    //
    // add new columns


    /// add a single column
    /// \{
/*
    template <class Column>
    void add_column_with_default(Column&& defaultval)
    {
      add_dense_column(defaultval);
    }
*/

    template <class Column>
    void add_column_with_default(dense<Column> defval_wrapper)
    {
      add_dense_column(std::move(defval_wrapper).value());
    }

    template <class Column>
    void add_column_with_default(sparse<Column> defval_wrapper)
    {
      add_sparse_column(std::move(defval_wrapper).value());
    }


    void name_column(size_t i, const string_t& name)
    {
      assert(allColumns && i < allColumns->size());

      (*allColNames)[name] = i;
    }

    void name_column(size_t i, std::string_view name)
    {
      name_column(i, persistent_string(name));
    }

    void name_last_column(const string_t& name)
    {
      assert(allColumns);

      const size_t idx = allColumns->size()-1;

      name_column(idx, name);
    }

    void name_last_column(std::string_view name)
    {
      name_last_column(persistent_string(name));
    }

/*
    void name_columns(const std::vector<string_t>& names)
    {
      name_columns_internal(names, &dataframe::name_column);
    }

    template <class StringT>
    void name_columns_std(const std::vector<StringT>& names)
    {
      name_columns_internal(names, &dataframe::name_column_std);
    }
*/
    /// \}

    /// add multiple columns
    /// \{
    template <class... Columns, size_t... I>
    void add_columns_default_value(std::tuple<Columns...>&& cols, std::index_sequence<I...>)
    {
      (add_column_with_default(std::get<I>(cols)), ...);
    }

    template <class... Columns>
    void add_columns_default_value(std::tuple<Columns...>&& cols)
    {
      add_columns_default_value(std::move(cols), std::make_index_sequence<sizeof... (Columns)>());
    }
    /// \}


    /// add columns without default value
    /// \note the preferred method is to use add_columns_default_value
    /// \deprecated
    /// \{

    template <class... Columns>
    void add_columns()
    {
      (add_column(tag<Columns>()), ...);
    }

    template <class... Columns>
    void add_columns(const std::tuple<Columns...>*)
    {
      add_columns<Columns...>();
    }

    /// \}

    template <class T>
    std::pair<typename dense_vector_t<T>::iterator, typename dense_vector_t<T>::iterator>
    get_dense_column(size_t col) const
    {
      ColumnRep          rep = column_at(col);
      dense_vector_t<T>& vec = accessors[rep.first].data<dense_vector_t<T> >(rep.second);

      return std::make_pair(vec.begin(), vec.end());
    }

    template <class T>
    std::pair<typename dense_vector_t<T>::iterator, typename dense_vector_t<T>::iterator>
    get_dense_column(const string_t& colname) const
    {
      return get_dense_column<T>(column_index(colname));
    }

    template <class T, class StringT>
    std::pair<typename dense_vector_t<T>::iterator, typename dense_vector_t<T>::iterator>
    get_dense_column_std(const StringT& colname) const
    {
      return get_dense_column<T>(column_index(colname));
    }

    template <class T>
    std::pair<typename sparse_vector_t<T>::iterator, typename sparse_vector_t<T>::iterator>
    get_sparse_column(size_t col) const
    {
      ColumnRep           rep = column_at(col);
      sparse_vector_t<T>& vec = accessors[rep.first].data<sparse_vector_t<T>, T>(rep.second);

      return std::make_pair(vec.begin(), vec.end());
    }

    template <class T>
    std::pair<typename sparse_vector_t<T>::iterator, typename sparse_vector_t<T>::iterator>
    get_sparse_column(const string_t& colname) const
    {
      return get_sparse_column<T>(column_index(colname));
    }

    template <class T, class StringT>
    std::pair<typename sparse_vector_t<T>::iterator, typename sparse_vector_t<T>::iterator>
    get_sparse_column_std(const StringT& colname) const
    {
      return get_sparse_column<T>(column_index(colname));
    }

    template <class T>
    std::pair<any_column_iterator<T>, any_column_iterator<T> >
    get_any_column(size_t col) const
    {
      ColumnRep rep = column_at(col);

      return accessors[rep.first].range<T>(rep.second);
    }

    template <class T>
    std::pair<any_column_iterator<T>, any_column_iterator<T> >
    get_any_column(const string_t& colname) const
    {
      return get_any_column<T>(column_index(colname));
      // return get_sparse_column<T>(column_index(colname));
    }

    template <class T, class StringT>
    std::pair<any_column_iterator<T>, any_column_iterator<T> >
    get_any_column_std(const StringT& colname) const
    {
      return get_any_column<T>(column_index(colname));
    }

    column_variant
    get_column_variant(size_t col) const
    {
      ColumnRep rep = column_at(col);

      return column_variant{accessors[rep.first], rep.second};
    }

    column_variant
    get_column_variant(const string_t& colname) const
    {
      return get_column_variant(column_index(colname));
    }

    template <class StringT>
    column_variant
    get_column_variant_std(const StringT& colname) const
    {
      return get_column_variant(column_index(colname));
    }

    std::vector<column_variant>
    get_column_variants() const
    {
      std::vector<column_variant> res;

      for (int idx = 0, lim = columns(); idx < lim; ++idx)
        res.emplace_back(get_column_variant(idx));

      return res;
    }


    template <class StringT>
    std::vector<column_variant>
    get_column_variants_std(const std::vector<StringT>& colnames) const
    {
      std::vector<column_variant> res;

      for (const StringT& col : colnames)
        res.emplace_back(get_column_variant_std(col));

      return res;
    }


    std::vector<column_desc>
    get_column_descriptors(const std::vector<int>& idxlst) const
    {
      std::vector<column_desc> res;

      for (int idx : idxlst)
        res.push_back(get_column_descriptor(idx));

      return res;
    }

    std::vector<std::string>
    get_column_names() const
    {
      assert(allColNames);

      std::vector<std::string> res{columns(), std::string{}};

      for (auto& colname : (*allColNames))
        res.at(colname.second) = std::string{colname.first.c_str()};

      return res;
    }

    std::vector<column_desc>
    get_column_descriptors(const std::vector<string_t>& colnames)
    {
      return get_column_descriptors(get_index_list(colnames));
    }

    void persist() const
    {
      // persist all columns
      for (size_t max = allColumns->size(), col = 0; col < max; ++col)
      {
        ColumnRep rep = column_at(col);

        accessors[rep.first].persist(rep.second);
      }

      // persist column names
      //~ allColNames->persist(ColumNames::value_type{persistent_string(), 0});
      allColNames->persist();
    }

    dataframe_variant_t
    get_cell_variant(int row, int col) const
    {
      ColumnRep rep = column_at(col);

      return accessors[rep.first].cell_variant(rep.second, row);
    }

    dataframe_variant_t
    get_cell_variant(int row, std::string_view colname) const
    {
      return get_cell_variant(row, column_index(colname));
    }

    void clear()
    {
      for (int idx = 0, lim = columns(); idx < lim; ++idx)
        get_column_variant(idx).clear();
    }

/*
    template <class... RowType>
    void xchg(int row, std::tuple<RowType...>&);

    template <class ColType>
    void xchg(int row, int col, ColType&);
*/
  private:
    //
    // types

    using VoidOfsPtr   = metall::offset_ptr<void>;
    using ColumnOfsRep = std::pair<int, VoidOfsPtr>;
    using ColumnRep    = std::pair<int, void*>;
    using ColumnNames  = flat_map<string_t, size_t>;

    //
    // data

    // not in persistent memory
    metall::manager&              memmgr; // \todo consider using a shared pointer
    std::string                   key;

    // in persistent memory
    dense_vector_t<ColumnOfsRep>* allColumns  = nullptr; ///< stores all columns
    ColumnNames*                  allColNames = nullptr; ///< stores all column names
    size_t*                       numRows     = nullptr; ///< quick access to number of rows
                                                         ///  (no need to query from columns)

    //
    // constants
    static constexpr const char* const colnamesSuffix = "~names";
    static constexpr const char* const numrowsSuffix  = "~size";


    //
    // internal functions

    /// returns a descriptor (kind, void pointer to container) for a
    ///   specified column.
    /// \private
    ColumnRep
    column_at(size_t col) const
    {
      const ColumnOfsRep& desc = allColumns->at(col);

      return ColumnRep{desc.first, metall::to_raw_pointer(desc.second)};
    }

    size_t column_index(std::string_view name) const
    {
      using iterator = typename ColumnNames::iterator;

      assert(allColNames);

      auto lt = [](const ColumnNames::value_type& col, const std::string_view& name) -> bool
                {
                  return name.compare(0, col.first.size(), &*col.first.begin()) > 0;
                };
      iterator pos = std::lower_bound( allColNames->begin(), allColNames->end(), name, lt );

      if (name.compare(0, pos->first.size(), &*pos->first.begin()) < 0)
      {
        CXX_UNLIKELY;
        throw unknown_column_error("not a known column");
      }

      // *pos == name
      return pos->second;
    }

    size_t column_index(const string_t& name) const
    {
      using iterator = typename ColumnNames::iterator;

      assert(allColNames);

      iterator pos = allColNames->find(name);

      if (pos == allColNames->find_end())
      {
        CXX_UNLIKELY;
        throw unknown_column_error(name + " is not a known column");
      }

      return pos->second;
    }

    /// create a prototype value_type object for the flat map
    /// \note the prototype object is used for creating new elements
    ///       when persist is called. All prototype objects will be
    ///       overwritten during persist and never occur as elements.
    ///       The prototype object is different from the default-value in
    ///       the sense that the protoype should be as light-weight as
    ///       possible, whereas the default-value may be a user defined
    ///       complex object (such as a very long string).
    /// \{
    template <class T>
    typename sparse_vector_t<T>::value_type
    sparse_entry_prototype(const T*)
    {
      using ReturnType = typename sparse_vector_t<T>::value_type;

      return ReturnType{};
    }

    typename sparse_vector_t<string_t>::value_type
    sparse_entry_prototype(const string_t*)
    {
      using ReturnType = typename sparse_vector_t<string_t>::value_type;

      return ReturnType{0, persistent_string()};
    }

    /// \}


    template <class... ColType, size_t... I>
    std::tuple<ColType...>
    get_row(int row, std::index_sequence<I...>) const
    {
      return std::tuple<ColType...>{ get_cell(row, I, tag<ColType>())... };
    }

    template <class... ColType, size_t... I>
    std::tuple<ColType...>
    get_row_idxlst(int row, const std::vector<int>& idxlst, std::index_sequence<I...>) const
    {
      return std::tuple<ColType...>{ get_cell(row, idxlst.at(I), tag<ColType>())... };
    }

    template <class ColType>
    void
    add_col_val(ColType&& el, size_t col)
    {
      ColumnRep rep = column_at(col);

      accessors[rep.first].add<ColType>(rep.second, std::move(el));
    }

    void
    add_col_variant(dataframe_variant_t&& el, size_t col)
    {
      ColumnRep rep = column_at(col);

      accessors[rep.first].add_variant(rep.second, std::move(el));
    }

    template <class... RowType, size_t... I>
    void
    add_row(std::tuple<RowType...>&& el, std::index_sequence<I...>)
    {
      (add_col_val<RowType>(std::move(std::get<I>(el)), I), ...);
    }

    template <class Column>
    void add_column(Column*)
    {
      add_dense_column(Column{});
    }

    template <class Column>
    void add_column(const dense<Column>*)
    {
      add_dense_column(Column{});
    }

    template <class Column>
    void add_column(const sparse<Column>*)
    {
      add_sparse_column(Column{});
    }

    template <class ColType, class T, class CtorArg>
    void add_column_generic(T defaultval, CtorArg extra)
    {
      // column_key := "key~num", where num is the index in allColumns
      std::string colkey{key};

      colkey.append('~', 1);
      colkey.append(std::to_string(allColumns->size()));

      const char* cKey = colkey.c_str();
      ColType* newcol = memmgr.construct<ColType>(cKey)( memmgr.get_allocator(),
                                                         std::move(defaultval),
                                                         extra
                                                       );

      assert(newcol);
      allColumns->emplace_back(column_traits<ColType>::col, newcol);
    }

    template <class T>
    void add_dense_column(T defaultval)
    {
      add_column_generic<dense_vector_t<T> >(std::move(defaultval), rows());
    }

    template <class T>
    void add_sparse_column(T defaultval)
    {
      add_column_generic<sparse_vector_t<T> >(std::move(defaultval), sparse_entry_prototype(tag<T>()));
    }

    template <class ColType>
    const ColType&
    get_cell(size_t row, size_t col, const ColType*) const
    {
      ColumnRep rep  = column_at(col);
      ColType*  elem = accessors[rep.first].at<ColType>(rep.second, row);

      if (!elem)
      {
        CXX_UNLIKELY;
        return accessors[rep.first].default_value<ColType>(rep.second);
        //~ throw std::logic_error("cell value not available");
      }

      return *elem;
    }

/*
    template <class ColType>
    std::optional<ColType>
    get_cell(size_t row, size_t col, const std::optional<ColType>*) const
    {
      ColumnRep rep = column_at(col);
      ColType*  elem = accessors[rep.first].at<ColType>(rep.second, row);

      if (!elem)
      {
        CXX_UNLIKELY;
        return std::optional<ColType>{};
      }

      return *elem;
    }
*/

    column_desc
    get_column_descriptor(int col) const
    {
      ColumnRep          rep = column_at(col);
      vector_accessor_any& column = accessors[rep.first];

      return column_desc{column.type_name(), column.is_sparse()};
    }

    template <class StringT>
    void name_columns_internal( const std::vector<StringT>& names,
                                void (dataframe::*fn) (size_t, const StringT& el)
                              )
    {
      for (size_t max = names.size(), i = 0; i < max; ++i)
        (this->*fn)(i, names[i]);
    }
};

template <class Fn, class T>
void callback_cell( Fn& fn,
                    size_t row, size_t col,
                    std::pair<any_column_iterator<T>, any_column_iterator<T> >& range
                  )
{
  assert((range.first == range.second) || (range.first.row() >= row));

  if (!(range.first == range.second) && range.first.row() == row)
  {
    fn(row, col, std::optional<T>{*range.first});
    ++range.first;
  }
  else
  {
    fn(row, col, std::optional<T>{});
  }
}

template <class Fn, class T>
void callback_cell( Fn& fn,
                    size_t row, size_t col,
                    std::pair< typename sparse_vector_t<T>::iterator,
                               typename sparse_vector_t<T>::iterator
                             >& range
                  )
{
  assert((range.first == range.second) || (range.first->first >= row));

  if (!(range.first == range.second) && range.first->first == row)
  {
    fn(row, col, std::optional<T>{range.first->second});
    ++range.first;
  }
  else
  {
    fn(row, col, std::optional<T>{});
  }
}

template <class Fn, class T>
void callback_cell( Fn& fn,
                    size_t row, size_t col,
                    std::pair< typename dense_vector_t<T>::iterator,
                               typename dense_vector_t<T>::iterator
                             >& range
                  )
{
  assert(range.first != range.second);

  fn(row, col, std::optional<T>{*range.first});
  ++range.first;
}


template <class T>
std::optional<T>
valueOf(int row, std::pair<any_column_iterator<T>, any_column_iterator<T> >& range)
{
  assert((range.first == range.second) || (range.first.row() >= row));

  if (!(range.first == range.second) && (range.first.row() == row))
    return std::optional<T>{*(range.first++)};

  return std::optional<T>{};
}

template <class T>
std::optional<T>
valueOf( int row,
         std::pair< typename sparse_vector_t<T>::iterator,
                    typename sparse_vector_t<T>::iterator
                  >& range
       )
{
  assert((range.first == range.second) || (range.first->first >= row));

  if (!(range.first == range.second) && (range.first->first == row))
    return std::optional<T>{*(range.first++)};

  return std::optional<T>{};
}


template <class T>
std::optional<T>
valueOf( int /* row */,
         std::pair< typename dense_vector_t<T>::iterator,
                    typename dense_vector_t<T>::iterator
                  >& range
       )
{
  assert(range.first != range.second);

  return std::optional<T>{*(range.first++)};
}

template <class Iter>
std::optional<typename std::iterator_traits<Iter>::value_type>
_valueOf(int row, std::pair<Iter, Iter>& range)
{
  return valueOf<typename std::iterator_traits<Iter>::value_type>(row, range);
}


template <class T>
struct mapped_type
{
  using type = T;
};

template <class T>
struct mapped_type<std::pair<const size_t, T> >
{
  using type = T;
};

template <class Pair>
struct column_value_type {};


template <class Iter>
struct column_value_type<std::pair<Iter, Iter> >
{
  using type = typename mapped_type<typename std::iterator_traits<Iter>::value_type>::type;
};


template <class Fn, class... ColumnRange>
Fn foreach_cell(Fn fn, size_t rowlimit, ColumnRange... columns)
{
  for (size_t row = 0; row < rowlimit; ++row)
  {
    size_t col = 0;

    (callback_cell<Fn, typename column_value_type<ColumnRange>::type>(fn, row, col++, columns), ...);
  }

  return fn;
}

template <class Fn, class... ColumnRange>
Fn foreach_row(Fn fn, size_t rowlimit, ColumnRange... columns)
{
  for (size_t row = 0; row < rowlimit; ++row)
    fn(row, _valueOf(row, columns)...);

  return fn;
}


} // namespace experimental


