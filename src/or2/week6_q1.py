import string
import numpy as np
import pandas as pd
import pyomo.environ as pyo
from pyomo.opt import SolverStatus, TerminationCondition

from base import PandasFileConnector
from base import loguru_logger as _logger


class OptimisationModelQ1:

    def __init__(self):
        self._logger = _logger
        self.model = pyo.ConcreteModel()
        self.job_data, self.conflicting_jobs_data = self.get_data()

    def build_and_run(self):
        """Main method for building and running the optimisation model"""
        self._logger.info("Setting up model...")
        self._set_parameters()
        self._set_constraints()
        self._set_objective_function()
        self._logger.info("Model setup completed successfully.")

        self._logger.info("Solving model...")
        self._solve_model()
        self._logger.info("Model solved successfully.")

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
        """Defining model indices, sets, parameters, and decision variables"""
        self._logger.debug("Defining model indices and sets...")
        self.model.I = pyo.Set(initialize=[1, 2, 3])
        self.model.J = pyo.Set(initialize=[job for job in self.job_data['Job']])
        self.model.G = pyo.Set(initialize=self.conflicting_jobs_data.columns.values.tolist())
        self._logger.debug("Model indices and sets defined successfully.")

        self._logger.debug("Defining model parameters...")
        self.model.processing_time = pyo.Param(
            self.model.J,
            initialize={job_row['Job']: job_row['Processing time'] for job_row in
                        self.job_data.to_dict(orient='records')},
            domain=pyo.Any
        )
        self.model.conflicting_groups = pyo.Param(
            self.model.J, self.model.G,
            initialize={(j, g): self.conflicting_jobs_data.loc[self.conflicting_jobs_data.index == j, g]
                        for j in self.model.J for g in self.model.G},
            domain=pyo.Any, mutable=True
        )
        self._logger.debug("Model parameters defined successfully.")

        self._logger.debug("Defining model decision variables...")
        self.model.makespan = pyo.Var(domain=pyo.PositiveReals)
        self.model.x = pyo.Var(self.model.I, self.model.J, domain=pyo.Binary)
        self._logger.debug("Model decision variables defined successfully.")

    def _set_objective_function(self):
        """Defining objective function"""
        self._logger.debug("Defining model objective function...")
        self.model.objective = pyo.Objective(rule=self.__objective_function, sense=pyo.minimize)
        self._logger.debug("Model objective function defined successfully.")

    @staticmethod
    def __objective_function(model):
        """Minimise model makespan value"""
        return model.makespan

    def _set_constraints(self):
        """Defining model constraints"""
        self._logger.debug("Defining model constraints...")
        self.__makespan_constraint()
        self.__assign_only_to_one_machine()
        self.__avoid_conflicting_jobs()
        self._logger.debug("Model constraints defined successfully.")

    def __makespan_constraint(self):
        """Makespan (w) must be larger than the total processing time for each machine i"""
        self.model.makespan_constraint = pyo.ConstraintList()
        for i in self.model.I:
            self.model.makespan_constraint.add(
                self.model.makespan >=
                pyo.quicksum(self.model.processing_time[j] * self.model.x[i, j] for j in self.model.J)
            )

    def __assign_only_to_one_machine(self):
        """Ensures one job is only assigned to one machine"""
        self.model.assign_only_to_one_machine_constraint = pyo.ConstraintList()
        for j in self.model.J:
            self.model.assign_only_to_one_machine_constraint.add(
                pyo.quicksum(self.model.x[i, j] for i in self.model.I) == 1
            )

    def __avoid_conflicting_jobs(self):
        """Avoid conflicting jobs within the same group"""
        self.model.avoid_conflicting_jobs_constraint = pyo.ConstraintList()
        for i in self.model.I:
            for g in self.model.G:
                self.model.avoid_conflicting_jobs_constraint.add(
                    pyo.quicksum(self.model.x[i, j] * self.model.conflicting_groups[j, g] for j in self.model.J) <= 1
                )

    @staticmethod
    def get_data():
        job_data = PandasFileConnector.load("./data/or2/q1_data.csv")

        # Adding conflicting jobs together into a group
        job_data['Conflicting jobs'] = job_data['Conflicting jobs'].replace('None', np.nan)
        conflicting_jobs_data = job_data.loc[~job_data['Conflicting jobs'].isna(), :].copy()
        conflicting_jobs_data['Conflicting jobs group'] = \
            conflicting_jobs_data['Conflicting jobs'] + ', ' + conflicting_jobs_data['Job'].astype(str)
        conflicting_jobs_data = conflicting_jobs_data['Conflicting jobs group'].str.split(',', expand=True)

        conflicting_jobs_list = []
        for row_idx, group_row in conflicting_jobs_data.iterrows():
            group_row = pd.to_numeric(group_row, errors='coerce').dropna().astype(int).to_list()
            group_row.sort()
            if group_row not in conflicting_jobs_list:
                conflicting_jobs_list.append(group_row)
        conflicting_jobs_list = dict(zip(string.ascii_uppercase[:len(conflicting_jobs_list)], conflicting_jobs_list))

        job_data['Conflicting job group'] = np.nan
        for job in job_data['Job'].unique():
            for job_group, job_list in conflicting_jobs_list.items():
                if job in job_list:
                    job_data.loc[job_data['Job'] == job, 'Conflicting job group'] = job_group

        conflicting_jobs_df = job_data.pivot(
            index='Job', columns='Conflicting job group', values='Conflicting job group',
        )
        conflicting_jobs_df = conflicting_jobs_df.dropna(axis=1, how='all')
        conflicting_jobs_df[~conflicting_jobs_df.isna()] = 1
        conflicting_jobs_df[conflicting_jobs_df.isna()] = 0
        conflicting_jobs_df.columns.name = None

        return job_data, conflicting_jobs_df


if __name__ == '__main__':

    Q1_Model = OptimisationModelQ1()
    Q1_Model.build_and_run()

    print(f"(Objective Function) Makespan: {Q1_Model.model.makespan.extract_values()}")
