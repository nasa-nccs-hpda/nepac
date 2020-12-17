from core.model.CeleryConfiguration import *

inclModules.append('nepac.model.NepacProcessCelery')
app.conf.include = inclModules
app.conf.worker_concurrency = 8
