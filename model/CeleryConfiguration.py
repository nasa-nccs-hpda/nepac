from core.model.CeleryConfiguration import *


inclModules.append('nepac.model.NepacProcessCelery')
app.conf.setdefault(IL_LOGLEVEL, 'ERROR')
app.conf.include = inclModules
app.conf.worker_concurrency = 8
app.conf.worker_prefetch_multiplier = 1
