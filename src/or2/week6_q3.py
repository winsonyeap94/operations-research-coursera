from base import PandasFileConnector
from base import loguru_logger as _logger


class OptimisationModelQ3:

    def __init__(self, m):
        """
        Initialise optimisation model.

        Args:
            m: Number of ambulances to setup
        """
        self.m = m
        self._logger = _logger

    def run_heuristic_optimisation(self, distance_df, population_df):
        """Main function for running heuristic optimisation"""

        # Initialisation
        m_assigned = list()

        # Iterate heuristics until all ambulances are assigned
        self._logger.info("Starting iterations for heuristic optimisation...")
        while len(m_assigned) < self.m:
            m_assigned, max_pwft = self.heuristic_assignment(distance_df, population_df, m_assigned)
        self._logger.info(f"Heuristic optimisation completed successfully with ambulances assigned to {m_assigned} "
                          f"and Max PWFT of {max_pwft}")

        return max_pwft

    @staticmethod
    def heuristic_assignment(distance_df, population_df, m_assigned):
        """
        Heuristic logic for assigning ambulances to districts

        Follows the logic below:
        1. First, identify districts which do not currently have an ambulance.
        2. Next, assign ambulance to the district that may minimise the maximum PWFT among all districts.
        3. If there is a tie, then pick the one with the smallest District ID.
        """

        # Initialisation
        distance_df = distance_df.copy()
        population_df = population_df.copy()

        # Step 1: Identify districts which do not currently have an ambulance
        districts_without_ambulance = [x for x in distance_df.index.astype(str) if x not in m_assigned]

        # Step 2: Assign ambulance to the district that may minimise the maximum PWFT among all districts
        pwft_df = distance_df.apply(lambda x: x * population_df['Population'].values, axis=0)
        pwft_df = pwft_df.loc[
            pwft_df.index.astype(str).isin(districts_without_ambulance), districts_without_ambulance
        ].copy()
        max_pwft_df = pwft_df.max(axis=0)

        # Step 3: If there is a tie, then pick the one with the smallest District ID
        min_max_pwft_district = max_pwft_df.index[max_pwft_df == max_pwft_df.min()].min()
        m_assigned.append(min_max_pwft_district)

        max_pwft = max_pwft_df.min()

        return m_assigned, max_pwft

    @staticmethod
    def get_sample_data():
        """Loads sample data provided under Q3"""
        # District-to-district distance data
        q3_distance_df = PandasFileConnector.load("./data/or2/q3_distance_data.csv")
        q3_distance_df = q3_distance_df.set_index('District')

        # District population data
        q3_population_df = PandasFileConnector.load("./data/or2/q3_population_data.csv")

        return q3_distance_df, q3_population_df

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

    # Testing on sample data
    Q3_Model = OptimisationModelQ3(m=2)
    Q3_Model.run_heuristic_optimisation(*Q3_Model.get_sample_data())

    # Running on Q2 data
    Q3_Model = OptimisationModelQ3(m=3)
    q3_max_pwft = Q3_Model.run_heuristic_optimisation(*Q3_Model.get_data())

    # Comparing with model from Q3 for m=3
    from src.or2.week6_q2 import OptimisationModelQ2

    Q2_Model = OptimisationModelQ2(m=3)
    Q2_Model.build_and_run()

    q2_max_pwft = Q2_Model.model.max_pwft.extract_values()[None]

    optimality_gap = q3_max_pwft - q2_max_pwft
    print(f"Optimality Gap: {optimality_gap}")








