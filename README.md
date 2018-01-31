# Table of Contents

- [Introduction](#introduction)
- [Tools](#tools)
- [Command Line](#command-line)

# Introduction

YFImport is a tool to import Yahoo Finance data for experiments with StreamPref Data Stream Management System (DSMS) prototype.
YFImport is composed by a downloading tool and auxiliary tools to generated StreamPref environments for the execution of experiments.

# Tools

The YFImport is composed by individual tools for downloading of data and for execution of experiments with StreamPref DSMS prototype.
The tools are the following:
- __yfimport.py__: Tool for data download;
- __best.py__: Tool for experiments with __BEST__ and __TOPK__ operators (best tuples according to conditional preferences);
- __bestseq.py__: Tool for experiments with __BESTSEQ__ operator (best sequences according to temporal conditional preferences);
- __conseq.py__: Tool for experiments with __CONSEQ__ operator (subsequences with consecutive tuples);
- __endseq.py__: Tool for experiments with __ENDSEQ__ operator (subsequences with last position);
- __seq.py__: Tool for experiments with __SEQ__ operator (sequence extraction);

The experiments parameters must be updated directly in the source code.
Please see the related publications for more information.

# Command Line

Command line for __yfimport.py__ tool:

```
yfimport.py [-h] [-s START] [-e END] [-x EXCHANGE]
  -h, --help
		show the help message and exit
  -s START, --start START
		Start date yyyy-mm-dd(default: 365 days before end date)
  -e END, --end END
		End date yyyy-mm-dd(default: system current date)
  -x EXCHANGE, --exchange EXCHANGE
        	Filter by an exchange

```
