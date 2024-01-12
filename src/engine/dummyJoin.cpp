#include "dummyJoin.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "global/ValueId.h"
#include "ValuesForTesting.h"
#include "parser/ParsedQuery.h"

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
        : Operation(qec) {
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

dummyJoin::dummyJoin() {
    std::cout << "yet another test in constructor" << std::endl;
}

dummyJoin::dummyJoin(QueryExecutionContext* qec, SparqlTriple triple) {
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

void dummyJoin::addChild(std::shared_ptr<QueryExecutionTree> child) {
    
}

std::vector<QueryExecutionTree*> dummyJoin::getChildren() {

}


string dummyJoin::asStringImpl(size_t indent) const {
    std::string dummy = "Hallo";
    return dummy;
}

string dummyJoin::getDescriptor() const {
    std::string dummy = "Hallo";
    return dummy;
}

size_t dummyJoin::getResultWidth() const { // muss angepasst werden
    if (_verbose_init) {
        return _variablesLeft.size() + _variablesRight.size() - 1;
    } else {
        return 1;
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

ResultTable dummyJoin::computeResult() { // muss angepasst werden
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
        // soll einfach nur eine random result table erstellen, welche alles
        // enthält, sodass es mit resultwidth usw. passt
        // return {};
        // die resulttable kann ein leeres localvocab haben
        IdTable idtable = IdTable(1, _allocator);
        for (int i = 0; i < 10; i++) {
            ValueId toAdd = ValueId::makeFromInt(i);
            idtable.push_back({toAdd});
        }
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
    return {};
}