# contrib/pargres/Makefile

EXTENSION = pargres
PGFILEDESC = "Pargres - parallel query execution module"
MODULES = pargres
OBJS = pargres.o $(WIN32RES)
# REGRESS = aqo_disabled aqo_controlled aqo_intelligent aqo_forced aqo_learn

MODULE_big = pargres
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pargres
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

