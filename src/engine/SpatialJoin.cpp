#include "engine/SpatialJoin.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/VariableToColumnMap.h"
#include "global/ValueId.h"
#include "parser/ParsedQuery.h"
#include "util/AllocatorWithLimit.h"
#include "util/GeoSparqlHelpers.h"
#include "util/MemorySize/MemorySize.h"

// ____________________________________________________________________________
SpatialJoin::SpatialJoin(
    QueryExecutionContext* qec, SparqlTriple triple,
    std::optional<std::shared_ptr<QueryExecutionTree>> childLeft,
    std::optional<std::shared_ptr<QueryExecutionTree>> childRight)
    : Operation(qec),
      triple_{std::move(triple)},
      leftChildVariable_{triple_.s_.getVariable()},
      rightChildVariable_{triple_.o_.getVariable()} {
  parseMaxDistance();

  if (childLeft) {
    childLeft_ = std::move(childLeft.value());
  }
  if (childRight) {
    childRight_ = std::move(childRight.value());
  }

  AD_CONTRACT_CHECK(triple_.s_.isVariable() && triple_.o_.isVariable(),
                    "Currently, SpatialJoin needs two variables");
}

// ____________________________________________________________________________
void SpatialJoin::parseMaxDistance() {
  const std::string& input = triple_.p_._iri;
  std::string errormessage =
      "parsing of the maximum distance for the "
      "SpatialJoin operation was not possible";
  auto throwIfNotADigit = [&](const char a) {
    if (!isdigit(a)) {
      AD_THROW(errormessage);
    }
  };

  if (ctre::match<MAX_DIST_IN_METERS_REGEX>(input)) {
    std::string number = input.substr(
        MAX_DIST_IN_METERS.size(),
        input.size() - MAX_DIST_IN_METERS.size() - 1);  // -1: compensate for >
    std::ranges::for_each(number, throwIfNotADigit);
    maxDist_ = std::stoll(number);
  } else {
    AD_THROW(errormessage);
  }

  if (maxDist_ < 0) {
    AD_THROW("the maximum distance between two objects must be > 0");
  }
}

// ____________________________________________________________________________
std::shared_ptr<SpatialJoin> SpatialJoin::addChild(
    std::shared_ptr<QueryExecutionTree> child,
    const Variable& varOfChild) const {
  if (varOfChild == leftChildVariable_) {
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple_,
                                         std::move(child), childRight_);
  } else if (varOfChild == rightChildVariable_) {
    return std::make_shared<SpatialJoin>(getExecutionContext(), triple_,
                                         childLeft_, std::move(child));
  } else {
    AD_THROW("variable does not match");
  }
}

// ____________________________________________________________________________
bool SpatialJoin::isConstructed() const {
  if (childLeft_ && childRight_) {
    return true;
  }
  return false;
}

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> SpatialJoin::getChildren() {
  if (!(childLeft_ && childRight_)) {
    AD_THROW("SpatialJoin needs two children, but at least one is missing");
  }

  return {childLeft_.get(), childRight_.get()};
}

// ____________________________________________________________________________
string SpatialJoin::getCacheKeyImpl() const {
  if (childLeft_ && childRight_) {
    std::ostringstream os;
    os << "SpatialJoin\nChild1:\n" << childLeft_->getCacheKey() << "\n";
    os << "Child2:\n" << childRight_->getCacheKey() << "\n";
    os << "maxDist: " << maxDist_ << "\n";
    return std::move(os).str();
  } else {
    return "incomplete SpatialJoin class";
  }
}

// ____________________________________________________________________________
string SpatialJoin::getDescriptor() const {
  return absl::StrCat("SpatialJoin: ", triple_.s_.getVariable().name(),
                      " max distance of ", std::to_string(maxDist_), " to ",
                      triple_.o_.getVariable().name());
}

// ____________________________________________________________________________
size_t SpatialJoin::getResultWidth() const {
  if (childLeft_ && childRight_) {
    // don't subtract anything because of a common join column. In the case
    // of the spatial join, the join column is different for both sides (e.g.
    // objects with a distance of at most 500m to each other, then each object
    // might contain different positions, which should be kept).
    auto widthChildren =
        childLeft_->getResultWidth() + childRight_->getResultWidth();
    if (addDistToResult_) {
      return widthChildren + 1;
    } else {
      return widthChildren;
    }
  } else if (childLeft_ || childRight_) {
    // if only one of the children is added yet, the "dummy result table" only
    // consists of one result column, the not yet added variable
    return 1;
  } else {
    // if none of the children has been added yet, the "dummy result table"
    // consists of two columns: the variables, which need to be added
    return 2;
  }
}

// ____________________________________________________________________________
size_t SpatialJoin::getCostEstimate() {
  if (childLeft_ && childRight_) {
    size_t inputEstimate =
        childLeft_->getSizeEstimate() * childRight_->getSizeEstimate();
    if (useBaselineAlgorithm_) {
      return inputEstimate * inputEstimate;
    } else {
      // check after implementation, if it is correct, for now it remains
      // here because otherwise SonarQube complains about costEstimate and
      // sizeEstimate having the same implementation
      return inputEstimate *
             static_cast<size_t>(log(static_cast<double>(inputEstimate)));
    }
  }
  return 1;  // dummy return, as the class does not have its children yet
}

// ____________________________________________________________________________
uint64_t SpatialJoin::getSizeEstimateBeforeLimit() {
  if (childLeft_ && childRight_) {
    return childLeft_->getSizeEstimate() * childRight_->getSizeEstimate();
  }
  return 1;  // dummy return if not both children are added
}

// ____________________________________________________________________________
float SpatialJoin::getMultiplicity(size_t col) {
  auto getDistinctness = [](std::shared_ptr<QueryExecutionTree> child,
                            ColumnIndex ind) {
    auto size = (u_int)child->getSizeEstimate();
    auto multiplicity = child->getMultiplicity(ind);
    return static_cast<float>(size) / multiplicity;
  };

  if (col >= getResultWidth()) {
    AD_FAIL();
  }

  if (childLeft_ && childRight_) {
    std::shared_ptr<QueryExecutionTree> child;
    size_t column = col;
    if (addDistToResult_ && col == getResultWidth() - 1) {
      // as each distance is very likely to be unique (even if only after
      // a few decimal places), no multiplicities are assumed
      return 1;
    } else if (col < childLeft_->getResultWidth()) {
      child = childLeft_;
    } else {
      child = childRight_;
      column -= childLeft_->getResultWidth();
    }
    auto distinctnessChild = getDistinctness(child, column);
    return static_cast<float>(getSizeEstimate()) / distinctnessChild;
  } else {
    return 1;
  }
}

// ____________________________________________________________________________
bool SpatialJoin::knownEmptyResult() {
  // return false if either both children don't have an empty result, only one
  // child is available, which doesn't have a known empty result or neither
  // child is available
  return (childLeft_ && childLeft_->knownEmptyResult()) ||
         (childRight_ && childRight_->knownEmptyResult());
}

// ____________________________________________________________________________
vector<ColumnIndex> SpatialJoin::resultSortedOn() const {
  // the baseline (with O(n^2) runtime) can have some sorted columns, but as
  // the "true" computeResult method will use bounding boxes, which can't
  // guarantee that a sorted column stays sorted, this will return no sorted
  // column in all cases.
  return {};
}

long long SpatialJoin::getMaxDist() const { return maxDist_; }

// ____________________________________________________________________________
std::string SpatialJoin::betweenQuotes(std::string extractFrom) const {
  // returns everything between the first two quotes. If the string does
  // not contain two quotes, the string is returned as a whole
  //
  size_t pos1 = extractFrom.find("\"", 0);
  size_t pos2 = extractFrom.find("\"", pos1 + 1);
  if (pos1 != std::string::npos && pos2 != std::string::npos) {
    return extractFrom.substr(pos1 + 1, pos2 - pos1 - 1);
  } else {
    return extractFrom;
  }
}

std::string SpatialJoin::getPoint(const IdTable* restable, size_t row, ColumnIndex col) const {
  return betweenQuotes(
        ExportQueryExecutionTrees::idToStringAndType(
            getExecutionContext()->getIndex(), restable->at(row, col), {})
            .value()
            .first);
}

// ____________________________________________________________________________
long long SpatialJoin::computeDist(const IdTable* resLeft,
                                   const IdTable* resRight, size_t rowLeft,
                                   size_t rowRight, ColumnIndex leftPointCol,
                                   ColumnIndex rightPointCol) const {
  std::string point1 = getPoint(resLeft, rowLeft, leftPointCol);
  std::string point2 = getPoint(resRight, rowRight, rightPointCol);
  double dist = ad_utility::detail::wktDistImpl(point1, point2);
  return static_cast<long long>(dist * 1000);  // convert to meters
}

// ____________________________________________________________________________
void SpatialJoin::addResultTableEntry(IdTable* result,
                                      const IdTable* resultLeft,
                                      const IdTable* resultRight,
                                      size_t rowLeft, size_t rowRight,
                                      long long distance) const {
  // this lambda function copies elements from copyFrom
  // into the table res. It copies them into the row
  // rowIndRes and column column colIndRes. It returns the column number
  // until which elements were copied
  auto addColumns = [](IdTable* res, const IdTable* copyFrom, size_t rowIndRes,
                       size_t colIndRes, size_t rowIndCopy) {
    size_t col = 0;
    while (col < copyFrom->numColumns()) {
      res->at(rowIndRes, colIndRes) = (*copyFrom).at(rowIndCopy, col);
      colIndRes += 1;
      col += 1;
    }
    return colIndRes;
  };

  auto resrow = result->numRows();
  result->emplace_back();
  // add columns to result table
  size_t rescol = 0;
  rescol = addColumns(result, resultLeft, resrow, rescol, rowLeft);
  rescol = addColumns(result, resultRight, resrow, rescol, rowRight);

  if (addDistToResult_) {
    result->at(resrow, rescol) = ValueId::makeFromInt(distance);
    // rescol isn't used after that in this function, but future updates,
    // which add additional columns, would need to remember to increase
    // rescol at this place otherwise. If they forget to do this, the
    // distance column will be overwritten, the variableToColumnMap will
    // not work and so on
    // rescol += 1;
  }
}

ColumnIndex SpatialJoin::getJoinCol(const std::shared_ptr<const QueryExecutionTree>& child,
                       const Variable& childVariable) {
  auto varColMap =
      child->getRootOperation()->getExternallyVisibleVariableColumns();
  return varColMap[childVariable].columnIndex_;
}

// ____________________________________________________________________________
Result SpatialJoin::baselineAlgorithm() {
  auto getIdTable = [](std::shared_ptr<QueryExecutionTree> child) {
    std::shared_ptr<const Result> resTable = child->getResult();
    return &resTable->idTable();
  };

  const IdTable* resLeft = getIdTable(childLeft_);
  const IdTable* resRight = getIdTable(childRight_);
  ColumnIndex leftJoinCol = getJoinCol(childLeft_, leftChildVariable_);
  ColumnIndex rightJoinCol = getJoinCol(childRight_, rightChildVariable_);
  size_t numColumns = getResultWidth();
  IdTable result{numColumns, _executionContext->getAllocator()};

  // cartesian product with a distance of at most maxDist between the two
  // objects
  for (size_t rowLeft = 0; rowLeft < resLeft->size(); rowLeft++) {
    for (size_t rowRight = 0; rowRight < resRight->size(); rowRight++) {
      long long dist = computeDist(resLeft, resRight, rowLeft, rowRight,
                                   leftJoinCol, rightJoinCol);
      if (dist <= maxDist_) {
        addResultTableEntry(&result, resLeft, resRight, rowLeft, rowRight,
                            dist);
      }
    }
  }
  return Result(std::move(result), std::vector<ColumnIndex>{}, LocalVocab{});
}

// ____________________________________________________________________________
std::vector<box> SpatialJoin::computeBoundingBox(const point& startPoint) {
  // haversine function
  auto hav = [](double theta) {
    return (1 - std::cos(theta)) / 2;
  };

  // inverse haversine function
  auto archav = [](double theta) {
    return std::acos(1 - 2 * theta);
  };

  // safety buffer for numerical inaccuracies
  double maxDistInMetersBuffer = static_cast<double>(maxDist_);
  if (maxDist_ < 10) {
    maxDistInMetersBuffer = 10;
  } else if (maxDist_ < std::numeric_limits<long long>::max() / 1.02) {
    maxDistInMetersBuffer = 1.01 * maxDist_;
  } else {
    maxDistInMetersBuffer = std::numeric_limits<long long>::max();
  }
  
  // compute latitude bound
  double upperLatBound = startPoint.get<1>() + maxDistInMetersBuffer * (360 / circumference);
  double lowerLatBound = startPoint.get<1>() - maxDistInMetersBuffer * (360 / circumference);
  bool poleReached = false;
  // test for "overflows"
  if (lowerLatBound <= -90) {
    lowerLatBound = -90;
    poleReached = true;  // south pole reached
  }
  if (upperLatBound >= 90) {
    upperLatBound = 90;
    poleReached = true;  // north pole reached
  }
  if (poleReached) {
    return {box(point(-180.0f, lowerLatBound), point(180.0f, upperLatBound))};
  }

  // compute longitude bound. For an explanation of the calculation and the
  // naming convention see my master thesis
  double alpha = maxDistInMetersBuffer / radius;
  double gamma = (90 - std::abs(startPoint.get<1>())) * (2 * std::numbers::pi / 360);
  double beta = std::acos((std::cos(gamma) / std::cos(alpha)));
  double delta = 0;
  if (maxDistInMetersBuffer > circumference / 20) {
    // use law of cosines
    delta = std::acos((std::cos(alpha) - std::cos(gamma) * std::cos(beta))
                                / (std::sin(gamma) * std::sin(beta)));
  } else {
    // use law of haversines for numerical stability
    delta = archav((hav(alpha - hav(gamma - beta))) / (std::sin(gamma) * std::sin(beta)));
  }
  double lonRange = delta * 360 / (2 * std::numbers::pi);
  double leftLonBound = startPoint.get<0>() - lonRange;
  double rightLonBound = startPoint.get<0>() + lonRange;
  // test for "overflows" and create two bounding boxes if necessary
  if (leftLonBound < -180) {
    box box1 = box(point(-180, lowerLatBound),
                  point(rightLonBound, upperLatBound));
    box box2 = box(point(leftLonBound + 360, lowerLatBound),
                  point(180, upperLatBound));
    return {box1, box2};
  } else if (rightLonBound > 180) {
    box box1 = box(point(leftLonBound, lowerLatBound),
                  point(180, upperLatBound));
    box box2 = box(point(-180, lowerLatBound),
                  point(rightLonBound - 360, upperLatBound));
    return {box1, box2};
  }
  // default case, when no bound has an "overflow"
  return {box(point(leftLonBound, lowerLatBound),
              point(rightLonBound, upperLatBound))};
}

// ____________________________________________________________________________
bool SpatialJoin::containedInBoundingBoxes(const std::vector<box>& bbox, point point1) {
  // correct lon bounds if necessary
  while (point1.get<0>() < -180) {
    point1.set<0>(point1.get<0>() + 360);
  } 
  while (point1.get<0>() > 180) {
    point1.set<0>(point1.get<0>() - 360);
  }
  if (point1.get<1>() < -90){
    point1.set<1>(-90);
  } else if (point1.get<1>() > 90) {
    point1.set<1>(90);
  }

  for (size_t i = 0; i < bbox.size(); i++) {
    if (boost::geometry::covered_by(point1, bbox.at(i))) {
      return true;
    }
  }
  return false;
}

Result SpatialJoin::boundingBoxAlgorithm() {
  std::shared_ptr<const Result> resTableLeft = childLeft_->getResult();
  std::shared_ptr<const Result> resTableRight = childRight_->getResult();
  const IdTable* resLeft = &resTableLeft->idTable();
  const IdTable* resRight = &resTableRight->idTable();
  auto varColMapLeft =
      childLeft_->getRootOperation()->getExternallyVisibleVariableColumns();
  auto varColMapRight =
      childRight_->getRootOperation()->getExternallyVisibleVariableColumns();
  ColumnIndex leftJoinCol =
      varColMapLeft[leftChildVariable_].columnIndex_;
      //varColMapLeft[Variable{"?point1"}].columnIndex_;  // use leftChildVariable again, see line above
  ColumnIndex rightJoinCol =
      varColMapRight[rightChildVariable_].columnIndex_;
      // varColMapRight[Variable{"?point2"}].columnIndex_;  // use rightChildVariable again, see line above
  // size_t numColumns = getResultWidth();
  size_t numColumns = childLeft_->getResultWidth() + childRight_->getResultWidth() + 1;
  IdTable result{numColumns, _executionContext->getAllocator()};

  // create r-tree for smaller result table
  auto smallerResult = resLeft;
  auto otherResult = resRight;
  bool leftResSmaller = true;
  auto smallerChild = childLeft_;
  auto otherChild = childRight_;
  auto smallerVariable = leftChildVariable_;
  auto otherVariable = rightChildVariable_;
  if (resLeft->numRows() > resRight->numRows()) {
    smallerResult = resRight;
    otherResult = resLeft;
    leftResSmaller = false;
    smallerChild = childRight_;
    otherChild = childLeft_;
    smallerVariable = rightChildVariable_;
    otherVariable = leftChildVariable_;
  }

  bgi::rtree<value, bgi::quadratic<16>> rtree;
  for (size_t i = 0; i < smallerResult->numRows(); i++) {
    // get point of row i
    ColumnIndex smallerJoinCol = getJoinCol(smallerChild, smallerVariable);
    std::string pointstr = getPoint(smallerResult, i, smallerJoinCol);  // todo 3 needs to be replaced by col, but i think thats easier when done in the real spatialjoin class
    pointstr = betweenQuotes(pointstr);
    auto [lng1, lat1] = ad_utility::detail::parseWktPoint(pointstr);
    point p(lng1, lat1);
    // add every point together with the row number into the rtree
    rtree.insert(std::make_pair(p, i));
  }
  for (size_t i = 0; i < otherResult->numRows(); i++) {
    ColumnIndex otherJoinCol = getJoinCol(otherChild, otherVariable);
    std::string pointstr = getPoint(otherResult, i, otherJoinCol);  // todo 3 needs to be replaced by col, but i think thats easier when done in the real spatialjoin class
    pointstr = betweenQuotes(pointstr);
    auto [lng1, lat1] = ad_utility::detail::parseWktPoint(pointstr);
    point p(lng1, lat1);
    // query the other rtree for every point using the following bounding box
    std::vector<box> bbox = computeBoundingBox(p);
    std::vector<value> results;
    for (size_t k = 0; k < bbox.size(); k++) {
      rtree.query(bgi::intersects(bbox.at(k)), std::back_inserter(results));
    }
    for (size_t k = 0; k < results.size(); k++) {
      size_t rowLeft = results.at(k).second;
      size_t rowRight = i;
      if (!leftResSmaller) {
        rowLeft = i;
        rowRight = results.at(k).second;
      }
      auto distance = computeDist(resLeft, resRight, rowLeft, rowRight, leftJoinCol, rightJoinCol);
      if (distance <= maxDist_) {
        addResultTableEntry(&result, resLeft, resRight, rowLeft, rowRight, distance);
      }
    }
  }
  Result resTable = Result(std::move(result), std::vector<ColumnIndex>{}, LocalVocab{});
  return resTable;
}

// ____________________________________________________________________________
Result SpatialJoin::computeResult([[maybe_unused]] bool requestLaziness) {
  if (useBaselineAlgorithm_) {
    return baselineAlgorithm();
  } else {
    return boundingBoxAlgorithm();
  }
}

// ____________________________________________________________________________
VariableToColumnMap SpatialJoin::computeVariableToColumnMap() const {
  VariableToColumnMap variableToColumnMap;
  auto makeUndefCol = makePossiblyUndefinedColumn;
  auto makeDefCol = makeAlwaysDefinedColumn;

  if (!(childLeft_ || childRight_)) {
    // none of the children has been added
    variableToColumnMap[leftChildVariable_] = makeUndefCol(ColumnIndex{0});
    variableToColumnMap[rightChildVariable_] = makeUndefCol(ColumnIndex{1});
  } else if (childLeft_ && !childRight_) {
    // only the left child has been added
    variableToColumnMap[rightChildVariable_] = makeUndefCol(ColumnIndex{1});
  } else if (!childLeft_ && childRight_) {
    // only the right child has been added
    variableToColumnMap[leftChildVariable_] = makeUndefCol(ColumnIndex{0});
  } else {
    auto addColumns = [&](std::shared_ptr<QueryExecutionTree> child,
                          size_t offset) {
      auto varColsmap = child->getVariableColumns();
      auto varColsVec = copySortedByColumnIndex(varColsmap);
      std::ranges::for_each(
          varColsVec,
          [&](const std::pair<Variable, ColumnIndexAndTypeInfo>& varColEntry) {
            if (varColEntry.second.mightContainUndef_ ==
                ColumnIndexAndTypeInfo::AlwaysDefined) {
              variableToColumnMap[varColEntry.first] = makeDefCol(
                  ColumnIndex{offset + varColEntry.second.columnIndex_});
            } else {
              AD_CONTRACT_CHECK(varColEntry.second.mightContainUndef_ ==
                                ColumnIndexAndTypeInfo::PossiblyUndefined);
              variableToColumnMap[varColEntry.first] = makeUndefCol(
                  ColumnIndex{offset + varColEntry.second.columnIndex_});
            }
          });
    };

    auto sizeLeft = childLeft_->getResultWidth();
    auto sizeRight = childRight_->getResultWidth();
    addColumns(childLeft_, 0);
    addColumns(childRight_, sizeLeft);

    if (addDistToResult_) {
      variableToColumnMap[Variable{nameDistanceInternal_}] =
          makeDefCol(ColumnIndex{sizeLeft + sizeRight});
    }
  }

  return variableToColumnMap;
}