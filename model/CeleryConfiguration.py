from core.model.CeleryConfiguration import *


inclModules.append('nepac.model.NepacProcessCelery')
app.conf.include = inclModules
app.conf.worker_concurrency = 10
app.conf.worker_prefetch_multiplier = 1
