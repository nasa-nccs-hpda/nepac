from core.model.CeleryConfiguration import *

inclModules.append('nepac.model.NepacProcessCelery')
app.conf.include = inclModules
#app.conf.result_serializer = 'pickle'
app.conf.worker_concurrency = 8