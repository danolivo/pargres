# contrib/pargres/Makefile

MODULE_big = pargres
EXTENSION = pargres
EXTVERSION = 0.1
PGFILEDESC = "Pargres - parallel query execution module [Prototype]"
MODULES = pargres
OBJS = pargres.o exchange.o connection.o hooks_exec.o common.o $(WIN32RES)
# REGRESS = aqo_disabled aqo_controlled aqo_intelligent aqo_forced aqo_learn

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

DATA_built = $(EXTENSION)--$(EXTVERSION).sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/pargres
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(EXTENSION)--$(EXTVERSION).sql: init.sql
	cat $^ > $@
