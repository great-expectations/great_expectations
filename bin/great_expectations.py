#!/usr/bin/env python

import argh

import great_expectations as ge

def initialize():
	raise NotImplementedError()

def validate():
	raise NotImplementedError()

argh.dispatch_commands([
	initialize,
	validate,
])