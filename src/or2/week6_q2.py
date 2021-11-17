import pyomo.environ as pyo
from pyomo.opt import SolverStatus, TerminationCondition

from base import PandasFileConnector
from base import loguru_logger as _logger


class OptimisationModelQ2:

    def __init__(self, m):
        """
        Initialise optimisation model.

        Args:
            m: Number of ambulances to setup
        """
        self.m = m
        self._logger = _logger
        self.model = pyo.ConcreteModel()
        self.distance_data, self.population_data = self.get_data()

    def build_and_run(self):
        """Main method for building and running the optimisation model"""
        self._logger.info("Setting up model...")
        self._set_parameters()
        self._set_constraints()
        self._set_objective_function()
        self._logger.info("Model setup completed successfully.")

        self._logger.info("Solving model...")
        self._solve_model()
        self._logger.info("Model solved successfully")

    def _solve_model(self, solver_type='cbc'):
        """Solve optimisation model"""
        opt = pyo.SolverFactory(solver_type)

        self._logger.debug("Solver starting...")
        results = opt.solve(self.model, tee=False)
        self.results = results
        self._logger.debug("Solver completed.")

        if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition == TerminationCondition.optimal):
            self._logger.debug("Solution is feasible and optimal")
            results.write()
        else:
            raise ValueError("Model resulted into an infeasible solution")
        self.model.optimised = True

    def _set_parameters(self):
        self._logger.debug("Defining model indices and sets...")
        self.model.D = pyo.Set(initialize=[district for district in self.distance_data.index])
        self._logger.debug("Model indices and sets defined successfully.")

        self._logger.debug("Defining model parameters...")
        self.model.district_distance = pyo.Param(
            self.model.D, self.model.D,
            initialize={
                (district_1, district_2):
                    self.distance_data.loc[self.distance_data.index == district_1, str(district_2)].values[0]
                for district_1 in self.model.D for district_2 in self.model.D
            }
        )
        self.model.population = pyo.Param(
            self.model.D, initialize={x['District']: x['Population'] for _, x in self.population_data.iterrows()}
        )
        self._logger.debug("Model parameters defined successfully.")

        self._logger.debug("Defining model decision variables...")
        self.model.x = pyo.Var(self.model.D, domain=pyo.Binary)
        self.model.y = pyo.Var(self.model.D, self.model.D, domain=pyo.Binary)
        self.model.pwft = pyo.Var(self.model.D, domain=pyo.NonNegativeReals)
        self.model.max_pwft = pyo.Var(domain=pyo.NonNegativeReals)
        self._logger.debug("Model decision variables defined successfully.")

    def _set_objective_function(self):
        """Defining objective function"""
        self._logger.debug("Defining model objective function...")
        self.model.objective = pyo.Objective(rule=self.__objective_function, sense=pyo.minimize)
        self._logger.debug("Model objective function defined successfully.")

    @staticmethod
    def __objective_function(model):
        """Minimise maximum population-weighted firefighting time (pwft)"""
        return model.max_pwft

    def _set_constraints(self):
        """Defining model constraints"""
        self._logger.debug("Defining model constraints...")
        self.__num_ambulances_constraint()
        self.__y_variable_constraint()
        self.__max_pwft_constraint()
        self.__pwft_constraint()
        self._logger.debug("Model constraints defined successfully.")

    def __num_ambulances_constraint(self):
        self.model.num_ambulances_constraint = pyo.ConstraintList()
        self.model.num_ambulances_constraint.add(
            pyo.quicksum(self.model.x[j] for j in self.model.D) == self.m
        )

    def __y_variable_constraint(self):
        self.model.y_variable_constraint = pyo.ConstraintList()
        for i in self.model.D:
            for j in self.model.D:
                self.model.y_variable_constraint.add(self.model.y[i, j] <= self.model.x[j])

        self.model.y_variable_single_assignment_constraint = pyo.ConstraintList()
        for i in self.model.D:
            self.model.y_variable_single_assignment_constraint.add(
                pyo.quicksum(self.model.y[i, j] for j in self.model.D) == 1
            )

    def __max_pwft_constraint(self):
        self.model.max_pwft_constraint = pyo.ConstraintList()
        for i in self.model.D:
            self.model.max_pwft_constraint.add(self.model.max_pwft >= self.model.pwft[i])

    def __pwft_constraint(self):
        self.model.pwft_constraint = pyo.ConstraintList()
        for i in self.model.D:
            self.model.pwft_constraint.add(
                self.model.pwft[i] >= pyo.quicksum(
                    self.model.district_distance[i, j] * self.model.y[i, j] * self.model.population[i]
                    for j in self.model.D
                )
            )

    @staticmethod
    def get_data():
        """Loads and preprocesses data for optimisation model"""
        # District-to-district distance data
        distance_df = PandasFileConnector.load("./data/or2/q2_distance_data.csv")
        distance_df = distance_df.set_index('District')

        # District population data
        population_df = PandasFileConnector.load("./data/or2/q2_population_data.csv")

        return distance_df, population_df


if __name__ == "__main__":

    Q2_Model = OptimisationModelQ2(m=3)
    Q2_Model.build_and_run()

    print(f"(Objective Function) Max. PWFT: {Q2_Model.model.max_pwft.extract_values()}")


