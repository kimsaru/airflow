import os
import dagfactory

dag_file = os.path.join(os.path.dirname(__file__), "../plugins/simple_dag.yaml")

dag_factory = dagfactory.DagFactory(dag_file)
dag_factory.generate_dags(globals())