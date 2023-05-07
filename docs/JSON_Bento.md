# JSON Bento

## Overview
JSON Bento is a memory-efficient JSON data store.
It takes `JSON value` instances and stores them in memory-efficient (compact) formats internally.

Users can access stored JSON values by using the [0, N) index as the vector container.

Here is a simple example:
```c++
#include <iostream>

// All JSON Bento headers are in json_bento.hpp
#include <json_bento/json_bento.hpp>

// Not necessary, just for this example
#include <boost/json/src.hpp>

int main () {
  // Create a JSON Bento container ('box')
  json_bento::box box;

  // Store a JSON object data
  // Bento can parse input string
  box.push_back({{"key1", "value1" }, { "key2", 42 }});
  std::cout << box[0] << std::endl;
  
  // Boost.JSON or Metall.JSON values are also accepted
  box.push_back(boost::json::value(3.14));
  std::cout << box[1] << std::endl;
    
  return 0;
}
```

## Accessor

As JSON data are stored in compact formats, users must read/update stored data via `accessors`.
All non-primitive JSON data types, i.e., `value`, `string`, `array`, `object`, and `key-value` (the component of `object`), have corresponding accessors.
However, accessors do not hold actual data, but only pointers/indices to the stored JSON values (thus, copying accessors is cheap and fast).

Accessors provide Boost.JSON (Metall JSON)-like interface to read and update stored JSON values,
such as  `is_bool()`, `as_int64()`, `emplace_object()`.
In `accessor` classes, `as_*()` and `emplace_*()` functions return another accessor _instance_
rather than references (&) to the underlying data.
In other world, users cannot use reference types to hold the values returned by `as_*()` and `emplace_*()` functions.

Here is an API comparison between Boost.JSON and JSON Bento:

```c++
boost::json::value bj;
auto& bj_array = bj.emplace_array();
bj_array.emplace_back(10);

json_bento::box box;
box.push_back(); // push back an empty (null) value
// As array is an accessor instance,
// 'auto&' cannot be used to hold the returned value.
auto array = box[0].emplace_array();
array.emplace_back(10); // update the array
```


## JSON Bento and Metall

All classes in JSON Bento can be stored in Metall datastore,
including the main `bento` class and `accessors`.

Here is how to use JSON Bento with Metall:

```c++
// Create a Metall manager
metall::manager manager(metall::create_only, "/tmp/datastore");

// Create a JSON Bento container
auto *box = manager.construct<json_bento::box>("bento")();

// Store a JSON object data
box->push_back({{"key1", "value1" }, { "key2", 42 }});
```

## Converting JSON Bento value to/from Boost.JSON value or Metall JSON value

### value_to() function

To convert a JSON Bento value to a Boost.JSON value or Metall JSON value,
`json_bento::value_to(...)` function is available.

Here is an example:

```c++
json_bento::box box;
// ... push some JSON values into the box ... //
auto value_accessor = box[0];

// Convert a JSON Bento value to a Boost.JSON value
boost::json::value boost_json_value;
json_bento::value_to(value_accessor, boost_json_value);

// Convert a JSON Bento value to a Metall JSON value
metall::json::value mj_value;
json_bento::value_to(value_accessor, mj_value);

// If the target JSON value is default constructive,
// the conversion can also be done as follows, for example:
auto boost_json_value = json_bento::value_to<boost::json::value>(value_accessor);
```

### value_from() function

For the other way around, i.e., converting a Boost.JSON value or Metall JSON value to a JSON Bento value,
`json_bento::value_from(...)` function is available.

```c++
// Convert a Boost.JSON value to a JSON Bento value
boost::json::value boost_json_value;
// ... set some JSON data to the value ... //
json_bento::box box;
box.push_back(); // push back an empty (null) value
auto value_accessor = box[0];
json_bento::value_from(boost_json_value, value_accessor);
```

Converting a Metall JSON value to a JSON Bento value is similar to the Boost.JSON case.