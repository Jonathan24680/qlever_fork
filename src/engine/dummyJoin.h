#pragma once

#include "Operation.h"
#include "parser/ParsedQuery.h"

class dummyJoin : public Operation {
  public:
    dummyJoin();
    dummyJoin(QueryExecutionContext* qec, SparqlTriple triple);
    dummyJoin(QueryExecutionContext* qec,
      std::shared_ptr<QueryExecutionTree> t1,
      std::shared_ptr<QueryExecutionTree> t2,
      ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
      bool keepJoinColumn);
    dummyJoin(QueryExecutionContext* qec,
      std::shared_ptr<QueryExecutionTree> t1,
      std::shared_ptr<QueryExecutionTree> t2,
      ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
      bool keepJoinColumn, IdTable leftChildTable,
      std::vector<std::optional<Variable>> variablesLeft,
      IdTable rightChildTable,
      std::vector<std::optional<Variable>> variablesRight);
    shared_ptr<const ResultTable> geoJoinTest();
    virtual std::vector<QueryExecutionTree*> getChildren();
    // eindeutiger String für die Objekte der Klasse:
    virtual string asStringImpl(size_t indent = 0) const; 
    virtual string getDescriptor() const ; // für Menschen
    virtual size_t getResultWidth() const; // wie viele Variablen man zurückgibt
    virtual size_t getCostEstimate();
    virtual uint64_t getSizeEstimateBeforeLimit();
    virtual float getMultiplicity(size_t col);
    virtual bool knownEmptyResult();
    [[nodiscard]] virtual vector<ColumnIndex> resultSortedOn() const;
    virtual ResultTable computeResult();
    virtual VariableToColumnMap computeVariableToColumnMap() const;
    void addChild(std::shared_ptr<QueryExecutionTree> child,
                  Variable varOfChild);
  // don't make them private for testing purposes
  // private:
    std::shared_ptr<QueryExecutionTree> _left;
    std::shared_ptr<QueryExecutionTree> _right;
    ColumnIndex _leftJoinCol;
    ColumnIndex _rightJoinCol;
    bool _keepJoinColumn;
    std::vector<std::optional<Variable>> _variablesLeft;
    std::vector<std::optional<Variable>> _variablesRight;
    bool _verbose_init = false;
    ad_utility::MemorySize _limit = ad_utility::MemorySize::bytes(100000);
    ad_utility::AllocatorWithLimit<ValueId> _allocator =
            ad_utility::makeAllocatorWithLimit<ValueId>(_limit);
    std::optional<Variable> leftChildVariable = std::nullopt;
    std::optional<Variable> rightChildVariable = std::nullopt;
    std::shared_ptr<QueryExecutionTree> childLeft = nullptr;
    std::shared_ptr<QueryExecutionTree> childRight = nullptr;
};