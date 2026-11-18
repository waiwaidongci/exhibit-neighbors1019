from ..services import runs
class RunsController:
    list=staticmethod(runs.list_runs)
    create=staticmethod(runs.create_run)
    @staticmethod
    def get(db,id):return runs.public(runs.get_run(db,id),full=True)
