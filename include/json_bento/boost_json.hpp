// Copyright 2023 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#if defined(DOXYGEN_SKIP)
/// \brief If defined, link with a buit Boost.JSON.
#define JSON_BENTO_LINK_WITH_BOOST_JSON

/// \brief Include guard for boost/json/src.hpp
#define JSON_BENTO_BOOST_JSON_SRC_INCLUDED
#endif

#if JSON_BENTO_LINK_WITH_BOOST_JSON
#include <boost/json.hpp>
#else
#ifndef JSON_BENTO_BOOST_JSON_SRC_INCLUDED
#define JSON_BENTO_BOOST_JSON_SRC_INCLUDED
#include <boost/json/src.hpp>
#endif  // JSON_BENTO_BOOST_JSON_SRC_INCLUDED
#endif  // JSON_BENTO_LINK_WITH_BOOST_JSON
