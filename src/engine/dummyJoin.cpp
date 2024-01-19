#include "dummyJoin.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "global/ValueId.h"
#include "ValuesForTesting.h"
#include "parser/ParsedQuery.h"
#include "VariableToColumnMap.h"

// soll eine Klasse sein, welche aus dem nichts ein Ergebnis erzeugt

// note that this class is taking some unnecessary parameters for testing
dummyJoin::dummyJoin(QueryExecutionContext* qec,
            std::shared_ptr<QueryExecutionTree> t1,
            std::shared_ptr<QueryExecutionTree> t2,
            ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
            bool keepJoinColumn,
            IdTable leftChildTable,
            std::vector<std::optional<Variable>> variablesLeft,
            IdTable rightChildTable,
            std::vector<std::optional<Variable>> variablesRight)
          : Operation(qec){
  /* ValuesForTesting operationLeft = ValuesForTesting(qec,
      std::move(leftChildTable), std::move(variablesLeft));
  ValuesForTesting operationRight = ValuesForTesting(qec,
      std::move(rightChildTable), std::move(variablesRight)); */
  _left = ad_utility::makeExecutionTree<ValuesForTesting>(qec,
      std::move(leftChildTable),variablesLeft);
  _right = ad_utility::makeExecutionTree<ValuesForTesting>(qec,
      std::move(rightChildTable), variablesRight);
  _leftJoinCol = t1JoinCol;
  _rightJoinCol = t2JoinCol;
  _keepJoinColumn = keepJoinColumn;
  _variablesLeft = std::move(variablesLeft);
  _variablesRight = std::move(variablesRight);
  _verbose_init = true;
  std::cout << "test in constructor" << std::endl;
}

dummyJoin::dummyJoin(QueryExecutionContext* qec,
            std::shared_ptr<QueryExecutionTree> t1,
            std::shared_ptr<QueryExecutionTree> t2,
            ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
            bool keepJoinColumn)
        : Operation(qec),
        _left(t1),
        _right(t2),
        _leftJoinCol(t1JoinCol),
        _rightJoinCol(t2JoinCol),
        _keepJoinColumn(keepJoinColumn) {
  std::cout << "another test in constructor" << std::endl;
}

dummyJoin::dummyJoin() {  // never use as qec of operation is not initialized
    std::cout << "yet another test in constructor" << std::endl;
}

dummyJoin::dummyJoin(QueryExecutionContext* qec, SparqlTriple triple) // delete this constructor
      : Operation(qec) {
  std::cout << "so many tests in constructor" << std::endl;
  std::cout << triple._s << " " << triple._p << " " << triple._o
            << " " << std::endl;
    
  if (triple._s.isVariable() && triple._o.isVariable()) {
    leftChildVariable = triple._s.getVariable();
    rightChildVariable = triple._o.getVariable();
  } else {
    AD_THROW("SpatialJoin needs two variables");
  }
}

dummyJoin::dummyJoin(QueryExecutionContext* qec, SparqlTriple triple,
  std::optional<std::shared_ptr<QueryExecutionTree>> childLeft,
  std::optional<std::shared_ptr<QueryExecutionTree>> childRight)
            : Operation(qec) {
  this->triple = triple;
  if (childLeft) {
    this->childLeft = childLeft.value();
  }
  if (childRight) {
    this->childRight = childRight.value();
  }
  
  if (triple._s.isVariable() && triple._o.isVariable()) {
    leftChildVariable = triple._s.getVariable();
    rightChildVariable = triple._o.getVariable();
  } else {
    AD_THROW("SpatialJoin needs two variables");
  }
}

std::shared_ptr<dummyJoin> dummyJoin::addChild(
      std::shared_ptr<QueryExecutionTree> child, Variable varOfChild){
  if (varOfChild == leftChildVariable) {
    childLeft = child;
    return std::make_shared<dummyJoin>(getExecutionContext(), triple.value(),
          childLeft, childRight);
  } else if (varOfChild == rightChildVariable) {
    childRight = child;
    return std::make_shared<dummyJoin>(getExecutionContext(), triple.value(),
          childLeft, childRight);
  } else {
    LOG(INFO) << "variable does not match" << varOfChild._name << std::endl;
    AD_THROW("variable does not match");
  }
}

bool dummyJoin::isConstructed() {
  // if the spatialJoin has both children its construction is done. Then true
  // is returned. This is needed in the QueryPlanner, so that the QueryPlanner
  // stops trying to add children, after it is already constructed
  if (childLeft && childRight) {
    return true;
  }
  return false;
}

std::vector<QueryExecutionTree*> dummyJoin::getChildren() {
  if (!(childLeft || childRight)) {
    AD_THROW("SpatialJoin needs two variables");
  }

  return {childLeft.get(), childRight.get()};
}


string dummyJoin::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; i++) {
    os << " ";
  }
  os << "dummyJoin\nChild1:\n" << childLeft->asString(indent) << "\n";
  os << "Child2:\n" << childRight->asString(indent) << "\n";
  return std::move(os).str();
}

string dummyJoin::getDescriptor() const {
  return "Descriptor of dummyJoin";
}

size_t dummyJoin::getResultWidth() const {
  if (childLeft && childRight) {
    // don't subtract anything because of a common join column. In the case
    // of the spatial join, the join column is different for both sides (e.g.
    // objects with a distance of at most 500m to each other, then each object
    // might contain different positions, which should be kept)
    return childLeft->getResultWidth() + childRight->getResultWidth();
  } else if (_verbose_init) {
    return _variablesLeft.size() + _variablesRight.size() - 1;
  } else {
    return 1;  // dummy return, as the class does not have its children yet
  }
}

size_t dummyJoin::getCostEstimate() {
  return 1;
}

uint64_t dummyJoin::getSizeEstimateBeforeLimit() {
  return 1;
}

float dummyJoin::getMultiplicity(size_t col) {
  return 1;
}

bool dummyJoin::knownEmptyResult() {
  return false;
}

vector<ColumnIndex> dummyJoin::resultSortedOn() const {
  return {};
}

ResultTable dummyJoin::computeResult() {
  if (_verbose_init) {
      // compute the result based on the two child results
      // this assumes, that the IDtables are already sorted on the join column
      IdTable result(getResultWidth(), _allocator);
      size_t row_index_result = 0;

      std::shared_ptr<const ResultTable> res_left = _left->getResult();
      std::shared_ptr<const ResultTable> res_right = _right->getResult();
      if (res_left->size() == 0 || res_right->size() == 0) {
          return ResultTable(std::move(result), {}, {});
      } else {
          const IdTable* ida = &res_left->idTable();
          const IdTable* idb = &res_right->idTable();
          size_t row_a = 0;
          size_t row_b = 0;
          while (row_a < ida->size() && row_b < idb->size()) {
              ValueId a = (*ida).at(row_a, _leftJoinCol);
              ValueId b = (*idb).at(row_b, _rightJoinCol);
              if (a == b) {
                  /* this lambda function copies elements from copyFrom
                  * into the table res. It copies them into the row
                  * row_ind_res and column column col_ind_res. If the column
                  * is equal to the joinCol, the column is not copied, but
                  * skipped
                  */
                  auto addColumns = [](IdTable* res,
                                      const IdTable* copyFrom,
                                      size_t row_ind_res,
                                      size_t col_ind_res,
                                      size_t row_ind_copy,
                                      size_t joinCol) {
                      size_t col = 0;
                      while (col < copyFrom->numColumns()) {
                          if (col != joinCol) {
                              res->at(row_ind_res, col_ind_res) = 
                                  (*copyFrom).at(row_ind_copy, col);
                              col_ind_res += 1;
                          }
                          col += 1;
                      }
                      return col_ind_res;
                  };
                  // add row
                  result.emplace_back();
                  size_t rescol = 0;
                  if (_keepJoinColumn) {
                      result.at(row_index_result, rescol) = a;
                      rescol += 1;
                  }
                  // add columns of left subtree
                  rescol = addColumns(&result, ida, row_index_result,
                                      rescol, row_a, _leftJoinCol);
                  // add columns of right subtree
                  rescol = addColumns(&result, idb, row_index_result,
                                      rescol, row_b, _rightJoinCol);
                  
                  row_index_result += 1;
                  
                  row_a += 1;
                  row_b += 1;
              } else if (a < b) {
                  row_a += 1;
              } else if (a > b) {
                  row_b += 1;
              }
          }
      return ResultTable(std::move(result), {}, {});
  }
  } else {
    IdTable idtable = IdTable(0, _allocator);
    idtable.setNumColumns(getResultWidth());

    std::shared_ptr<const ResultTable> res_left = childLeft->getResult();
    std::shared_ptr<const ResultTable> res_right = childRight->getResult();

    const IdTable* idLeft = &res_left->idTable();
    const IdTable* idRight = &res_right->idTable();
    size_t numRowsLeft = idLeft->size();
    size_t numColsLeft = idLeft->numColumns();
    
    for (size_t i = 0; i < numRowsLeft; i++) {
      // iterate through all rows of the left id table
      for (size_t k = 0; k < idRight->size(); k++) {
        idtable.emplace_back();
        // for each row of the left idtable add a row of the right idtable
        for (size_t l = 0; l < idLeft->numColumns(); l++) {
          // add coloms from the left id table
          idtable[i * numRowsLeft + k][l] = idLeft->at(i, l);
        }
        for (size_t m = 0; m < idRight->numColumns(); m++) {
          // add columns from the right id table
          idtable[i * numRowsLeft + k][numColsLeft + m] = idRight->at(k, m);
        }
      }
    }

    /*for (int i = 0; i < 10; i++) {
      idtable.emplace_back();
      for (int k = 0; k < getResultWidth(); k++) {
        ValueId toAdd = ValueId::makeFromInt(i * 100 + k);
        idtable[i][k] = toAdd;
      }
    } */
    LocalVocab lv = {}; // let's assume, this has no local vocabs
    std::vector<ColumnIndex> sortedBy = {0};
    return ResultTable(std::move(idtable), {}, std::move(lv));
  }
}

shared_ptr<const ResultTable> dummyJoin::geoJoinTest() {
  std::shared_ptr<ResultTable> res = std::make_shared<ResultTable>(
    ResultTable(IdTable(1, _allocator),
          std::vector<ColumnIndex>{}, LocalVocab{}));
  return res;
}

VariableToColumnMap dummyJoin::computeVariableToColumnMap() const {
  // welche Variable kommt zu welcher Spalte
  std::cout << "varColMap dummyJoin " << std::endl;
  VariableToColumnMap variableToColumnMap;
  auto makeCol = makePossiblyUndefinedColumn;
  
  /*Depending on the amount of children the operation returns a different
  VariableToColumnMap. If the operation doesn't have both children it needs
  to aggressively push the queryplanner to add the children, because the
  operation can't exist without them. If it has both children, it can return
  the variable to column map, which will be present, after the operation has
  computed its result*/
  if (!(childLeft || childRight)) {
    // none of the children has been added
    variableToColumnMap[leftChildVariable.value()] = makeCol(ColumnIndex{0});
    variableToColumnMap[rightChildVariable.value()] = makeCol(ColumnIndex{1});
  } else if (childLeft && !childRight) {
    // only the left child has been added
    variableToColumnMap[rightChildVariable.value()] = makeCol(ColumnIndex{1});
  } else if (!childLeft && childRight) {
    // only the right child has been added
    variableToColumnMap[leftChildVariable.value()] = makeCol(ColumnIndex{0});
  } else {
    // both children have been added to the Operation
    auto varColsLeftMap = childLeft->getVariableColumns();
    auto varColsRightMap = childRight->getVariableColumns();
    auto varColsLeftVec = copySortedByColumnIndex(varColsLeftMap);
    auto varColsRightVec = copySortedByColumnIndex(varColsRightMap);
    size_t index = 0;
    for (size_t i = 0; i < varColsLeftVec.size(); i++) {
      variableToColumnMap[varColsLeftVec.at(i).first] = makeCol(ColumnIndex{index});
      index++;
    }
    for (size_t i = 0; i < varColsRightVec.size(); i++) {
      variableToColumnMap[varColsRightVec.at(i).first] = makeCol(ColumnIndex{index});
      index++;
    }
  }
  return variableToColumnMap;
}