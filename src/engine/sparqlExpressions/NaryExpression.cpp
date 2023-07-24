//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpression.h"

namespace sparqlExpression {
namespace detail {

// _____________________________________________________________________________
template <typename Op>
requires(isOperation<Op>)
NaryExpression<Op>::NaryExpression(Children&& children)
    : _children{std::move(children)} {}

// _____________________________________________________________________________

template <typename NaryOperation>
requires(isOperation<NaryOperation>)
ExpressionResult NaryExpression<NaryOperation>::evaluate(
    EvaluationContext* context) const {
  auto resultsOfChildren = ad_utility::applyFunctionToEachElementOfTuple(
      [context](const auto& child) { return child->evaluate(context); },
      _children);

  // A function that only takes several `ExpressionResult`s,
  // and evaluates the expression.
  auto evaluateOnChildrenResults =
      std::bind_front(ad_utility::visitWithVariantsAndParameters,
                      evaluateOnChildrenOperands, NaryOperation{}, context);

  return std::apply(evaluateOnChildrenResults, std::move(resultsOfChildren));
}

// _____________________________________________________________________________
template <typename Op>
requires(isOperation<Op>)
std::span<SparqlExpression::Ptr> NaryExpression<Op>::children() {
  return {_children.data(), _children.size()};
}

// __________________________________________________________________________
template <typename Op>
requires(isOperation<Op>) [[nodiscard]] string NaryExpression<Op>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  string key = typeid(*this).name();
  for (const auto& child : _children) {
    key += child->getCacheKey(varColMap);
  }
  return key;
}

#define INSTANTIATE_NARY(N, X, ...) \
  template class NaryExpression<Operation<N, X, __VA_ARGS__>>

INSTANTIATE_NARY(2, FV<decltype(orLambda), EffectiveBooleanValueGetter>,
                 SET<SetOfIntervals::Union>);

INSTANTIATE_NARY(2, FV<decltype(andLambda), EffectiveBooleanValueGetter>,
                 SET<SetOfIntervals::Intersection>);

INSTANTIATE_NARY(1, FV<decltype(unaryNegate), EffectiveBooleanValueGetter>,
                 SET<SetOfIntervals::Complement>);

INSTANTIATE_NARY(1, FV<decltype(unaryMinus), NumericValueGetter>);

INSTANTIATE_NARY(2, FV<decltype(multiply), NumericValueGetter>);

INSTANTIATE_NARY(2, FV<decltype(divide), NumericValueGetter>);

INSTANTIATE_NARY(2, FV<decltype(add), NumericValueGetter>);

INSTANTIATE_NARY(2, FV<decltype(subtract), NumericValueGetter>);

INSTANTIATE_NARY(1,
                 FV<NumericIdWrapper<decltype(ad_utility::wktLongitude), true>,
                    StringValueGetter>);
INSTANTIATE_NARY(1,
                 FV<NumericIdWrapper<decltype(ad_utility::wktLatitude), true>,
                    StringValueGetter>);
INSTANTIATE_NARY(2, FV<NumericIdWrapper<decltype(ad_utility::wktDist), true>,
                       StringValueGetter>);

INSTANTIATE_NARY(1, FV<decltype(extractYear), DateValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(extractMonth), DateValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(extractDay), DateValueGetter>);
INSTANTIATE_NARY(1, FV<std::identity, StringValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(strlen), StringValueGetter>);
}  // namespace detail
}  // namespace sparqlExpression
