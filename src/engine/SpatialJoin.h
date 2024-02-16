#pragma once

#include "Operation.h"
#include "parser/ParsedQuery.h"

// This class is implementing a SpatialJoin operation. This operations joins
// two tables, using their positional column. If the distance of the two
// positions is less than a given maximum distance, the pair will be in the
// result table.
class SpatialJoin : public Operation {
  public:
    // creates a SpatialJoin operation. The triple is needed, to get the
    // variable names. Those are the names of the children, which get added
    // later. In addition to that, the SpatialJoin operation needs a maximum
    // distance, which two objects can be apart, which will still be accepted
    // as a match and therefore be part of the result table.
    SpatialJoin(QueryExecutionContext* qec, SparqlTriple triple,
        std::optional<std::shared_ptr<QueryExecutionTree>> childLeft_,
        std::optional<std::shared_ptr<QueryExecutionTree>> childRight_,
        int maxDist);
    std::vector<QueryExecutionTree*> getChildren() override;
    string getCacheKeyImpl() const override; 
    string getDescriptor() const override;
    size_t getResultWidth() const override;
    size_t getCostEstimate() override;
    uint64_t getSizeEstimateBeforeLimit() override;

    // this function assumes, that the complete cross product is build and
    // returned. If the SpatialJoin does not have both children yet, it just
    // returns one as a dummy return.
    float getMultiplicity(size_t col) override;
    
    bool knownEmptyResult() override;
    [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override;
    ResultTable computeResult() override;

    // Depending on the amount of children the operation returns a different
    // VariableToColumnMap. If the operation doesn't have both children it needs
    // to aggressively push the queryplanner to add the children, because the
    // operation can't exist without them. If it has both children, it can
    // return the variable to column map, which will be present, after the
    // operation has computed its result
    VariableToColumnMap computeVariableToColumnMap() const override;

    // this method creates a new SpatialJoin object, to which the child gets
    // added. The reason for this behavior is, that the QueryPlanner can then
    // still use the existing SpatialJoin object, to try different orders
    std::shared_ptr<SpatialJoin> addChild(
          std::shared_ptr<QueryExecutionTree> child, Variable varOfChild);
    
    // if the spatialJoin has both children its construction is done. Then true
    // is returned. This function is needed in the QueryPlanner, so that the
    // QueryPlanner stops trying to add children, after the SpatialJoin is
    // already constructed
    bool isConstructed();

  private:
    ad_utility::MemorySize _limit = ad_utility::MemorySize::bytes(100000000);
    ad_utility::AllocatorWithLimit<ValueId> _allocator =
            ad_utility::makeAllocatorWithLimit<ValueId>(_limit);
    std::optional<Variable> leftChildVariable = std::nullopt;
    std::optional<Variable> rightChildVariable = std::nullopt;
    std::shared_ptr<QueryExecutionTree> childLeft_ = nullptr;
    std::shared_ptr<QueryExecutionTree> childRight_ = nullptr;
    std::optional<SparqlTriple> triple_ = std::nullopt;
    int maxDist_ = 0;  // max distance in meters, 0 encodes an infinite distance
    // adds an extra column to the result, which contains the actual distance,
    // between the two objects
    bool addDistToResult = true;
    const string nameDistanceInternal = "?distOfTheTwoObjectsAddedInternally";
};
