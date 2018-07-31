# ptyhon command
PYTHONCMD=python
PIPCMD=pip
VIRTUALENV=ENV
GITTAG=$(shell git describe --abbrev=0 2> /dev/null || cat $(CURDIR)/.version 2> /dev/null || echo v0 2> /dev/null)

reset:
	-$(shell rm -rf $(VIRTUALENV))
	$(PIPCMD) install virtualenv
	virtualenv ENV
	$(shell source ENV/bin/activate)
	$(PIPCMD) install -r requirements.txt

deps:
	$(shell source ENV/bin/activate)
	$(PIPCMD) install -r requirements.txt

cov:
	$(shell source ENV/bin/activate)
	$(PYTHONCMD) _run.py
	
push:
	git checkout master
	git push origin master
	git checkout develop
	git push origin develop
	git push origin $(GITTAG)

