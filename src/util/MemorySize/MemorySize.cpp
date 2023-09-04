// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "util/MemorySize/MemorySize.h"

#include <absl/strings/str_cat.h>

#include <cinttypes>
#include <ranges>
#include <string_view>

#include "util/Cache.h"
#include "util/ConstexprMap.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/MemorySize/MemorySizeParser.h"
#include "util/MemorySize/generated/MemorySizeLanguageLexer.h"
#include "util/MemorySize/generated/MemorySizeLanguageParser.h"

namespace ad_utility {
// _____________________________________________________________________________
std::string MemorySize::asString() const {
  // Convert number and memory unit name to the string, we want to return.
  auto toString = [](const auto number, std::string_view unitName) {
    return absl::StrCat(number, " ", unitName);
  };

  /*
  Choosing the memory unit type is done by choosing the unit type, in which
  range `memoryInBytes_` is contained.
  A memory unit type normally has the range `[hisSize,
  sizeOfTheNextBiggerUnit)`.
  Only exceptions are:
  - `TB`, which has no upper bound, because it's our biggest unit.
  - `kB`, which has the lower bound `100'000` instead of `1'000`. Typically, for
  such small sizes you still want the exact value because they mean something
  ,e.g. a block size or a page size etc..
  */
  constexpr ad_utility::ConstexprMap<char, size_t, 4> memoryUnitLowerBound(
      {std::pair<char, size_t>{'k', ad_utility::pow(10, 5)},
       std::pair<char, size_t>{'M', detail::numBytesPerUnit.at("MB")},
       std::pair<char, size_t>{'G', detail::numBytesPerUnit.at("GB")},
       std::pair<char, size_t>{'T', detail::numBytesPerUnit.at("TB")}});

  // Go through the units from top to bottom, in terms of size, and choose the
  // first one, that is smaller/equal to `memoryInBytes_`.
  if (memoryInBytes_ >= memoryUnitLowerBound.at('T')) {
    return toString(getTerabytes(), "TB");
  } else if (memoryInBytes_ >= memoryUnitLowerBound.at('G')) {
    return toString(getGigabytes(), "GB");
  } else if (memoryInBytes_ >= memoryUnitLowerBound.at('M')) {
    return toString(getMegabytes(), "MB");
  } else if (memoryInBytes_ >= memoryUnitLowerBound.at('k')) {
    return toString(getKilobytes(), "kB");
  } else {
    // Just return the amount of bytes.
    return toString(memoryInBytes_, "B");
  }
}

// _____________________________________________________________________________
MemorySize MemorySize::parse(std::string_view str) {
  return MemorySizeParser::parseMemorySize(str);
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const ad_utility::MemorySize& mem) {
  os << mem.asString();
  return os;
}

}  // namespace ad_utility