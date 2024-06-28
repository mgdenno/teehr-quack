#include "functions/functions.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <vector>
#include <iostream>

/*
create table test (obs float, sim float, grp varchar);
insert into test (obs, sim, grp) values (1.0, 1.1, 'A');
insert into test (obs, sim, grp) values (1.2, 1.3, 'A');
insert into test (obs, sim, grp) values (2.0, 2.1, 'A');
insert into test (obs, sim, grp) values (1.1, 1.3, 'B');
insert into test (obs, sim, grp) values (1.4, 1.0, 'B');
insert into test (obs, sim, grp) values (2.2, 2.4, 'B');
select grp, nash_sutcliffe(obs, sim) from test group by grp;
*/

namespace teehr {

using namespace duckdb;

template <class T>
struct NashSutcliffeState {
    T sum_squared_residuals;
    T observed_mean;
    std::vector< T > observations;
    int count;
};

struct NashSutcliffeOperation {
    template <class STATE>
    static void Initialize(STATE &state) {
        state.sum_squared_residuals = 0.0;
        state.observed_mean = 0.0;
        state.count = 0;
        state.observations = {};
    }

    // Operation method
    template <class A_TYPE, class B_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &observed, const B_TYPE &predicted, duckdb::AggregateBinaryInput &aggr_input_data) {
        state.count++;

		const double mean_differential = (observed - state.observed_mean) / state.count;
		const double new_observed_mean = state.observed_mean + mean_differential;
		state.observed_mean = new_observed_mean;

        const double residual = observed - predicted;
        state.sum_squared_residuals += residual * residual;

        // std::vector< double > observations;
        state.observations.push_back(observed);
    }

    // Combine method
    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE &target, duckdb::AggregateInputData &aggr_input_data) {
        if (target.count == 0) {
			target = source;
		} else if (source.count > 0) {
			const auto count = target.count + source.count;
			const auto observed_mean = (source.count * source.observed_mean + target.count * target.observed_mean) / count;
	
			target.observed_mean = observed_mean;
			target.count = count;

            const auto sum_squared_residuals = target.sum_squared_residuals + source.sum_squared_residuals;
            target.sum_squared_residuals = sum_squared_residuals;

            target.observations.insert( target.observations.end(), source.observations.begin(), source.observations.end() );

		}
    }

    // Finalize method
    template <class T, class STATE>
    static void Finalize(STATE &state, T &result, duckdb::AggregateFinalizeData &finalize_data) {
        if (state.count == 0) {
            finalize_data.ReturnNull();
        } else {
            T sum_squared_deviation_from_mean = 0;
            for (auto observation : state.observations) {
                sum_squared_deviation_from_mean += (observation - state.observed_mean) * (observation - state.observed_mean);
            }
            result = 1.0 - (state.sum_squared_residuals / sum_squared_deviation_from_mean);
        }
    }

    static bool IgnoreNull() { return true; }
};


duckdb::unique_ptr<duckdb::FunctionData> BindNashSutcliffe(
    duckdb::ClientContext &context, 
    duckdb::AggregateFunction &bound_function,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments
) {

    auto &observed_type = arguments[0]->return_type;
    auto &predicted_type = arguments[1]->return_type;

    bound_function = duckdb::AggregateFunction::BinaryAggregate<
            NashSutcliffeState<double>,
            double, 
            double, 
            double, 
            NashSutcliffeOperation
        >(
            observed_type, 
            predicted_type,
            predicted_type
        );

    bound_function.name = "nash_sutcliffe";
    return nullptr;
}

duckdb::AggregateFunction GetNashSutcliffeFunction(
    const duckdb::LogicalType &observed_type, 
    const duckdb::LogicalType &predicted_type
) {
    return duckdb::AggregateFunction::BinaryAggregate<
            NashSutcliffeState<double>, 
            double, 
            double, 
            double, 
            NashSutcliffeOperation
        >(
            observed_type, 
            predicted_type,
            predicted_type
        );
}

void NashSutcliffe::RegisterFunction(duckdb::Connection &conn, duckdb::Catalog &catalog) {
    // ...

    // Register Nash-Sutcliffe function
    duckdb::AggregateFunctionSet nash_sutcliffe("nash_sutcliffe");

    nash_sutcliffe.AddFunction(GetNashSutcliffeFunction(
            duckdb::LogicalType::DOUBLE,
            duckdb::LogicalType::DOUBLE
        )
    );
    duckdb::CreateAggregateFunctionInfo nash_sutcliffe_info(nash_sutcliffe);
    catalog.CreateFunction(*conn.context, nash_sutcliffe_info);
}


} // namespace hydro_duck