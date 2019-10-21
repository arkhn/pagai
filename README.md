# Pagai: dive into your data pool

[![Build Status](https://travis-ci.com/arkhn/fhir-store.svg?branch=master)](https://travis-ci.com/arkhn/fhir-store)
[![GitHub license](https://img.shields.io/github/license/arkhn/fhir-pipe.svg)](https://github.com/arkhn/fhir-pipe/blob/master/LICENSE)
[![Website arkhn.org](https://img.shields.io/website-up-down-green-red/https/arkhn.org.svg)](http://arkhn.org/)

The PagAI project is tightly linked to the fhir-ball project, but it serves also a broader purpose: providing an efficient tool to inspect a SQL database and extract meaningful informations.

## The concept

The pagai combines two advanced tools:

### Functional type inference of columns

The **column classifier** is a generic machine learning model which is used to determine the information contained in a column, that we call the _functional type_. Example of types are `firstname`, `name`, `address`, `city`, `id`, `date`, and `code` (like M/F genders). Our approach is to build sufficiently robust models that consider all this types as distinct but equivalent classes: this means that we won't provide a regex to extract a date for instance. Moreover, we focus on the column scale and not on the single item scale. This helps us to make the most of column data distributions and statistical signature of different types.

The strength of the classifier is that it can run different ML models under the hood. The current model is a RandomClassifier based on enhanced ngrams, but we're building a RNN-based model as well.

### The dependency graph builder

The **dependency graph** builder finds links between tables within a database, based on potential joins that could occur. This tool helps understanding which tables are linked with each other, like for example a table with patients and another with patient contact persons in case of emergency.

## The query functionality

Merging this two tools is the real strength of the pagai project. Indeed it is possible to query the database with the engine to retrieve interesting columns. The search is based on a score allocated to each column, which depends on the relevance of the functional type and the distance in the dependency graph to the table we're considering (like `patient` for example). In addition, we have also added a fuzzy matching algorithm on the table and column name to update the score and return the most relevant columns.

With this, we can answer questions like:
 - "Give me the date of birth of patient"
   > Api call `api/search/date/patient/birth`
 - "Give me the relation type between the patient and its contact person (husband, child, etc)"
   > Api call `api/search/code/patient/relation`
   
 NB: We mean the location of the column which has this information
 
 The api syntax is explained in the next section.

## The api

To make this tool easy to use for the largest number of people, with have built an api with the following structure:

    api/search/<functional_type>/<reference_table>/<keyword_column>

Parameters:
- **functional_type**: `firstname`, `name`, `address`, `city`, `id`, `date`, `code` and those of your imagination
- **reference_table**: the reference table for the dependency graph (`patient` in the examples above)
- **keyword_column**: keywords to match column or table names with fuzzy matching (ex: `birth`-> `birthdate`)

## Getting started and building my customized pagai

As for now, we're training our engine on a simplified version of the MIMIC dataset extended with firstname, name and address data.

Of course, it is possible to train the model and the graph with your own database. In particular, you can provide whatever functional type you want (you could add `phone` in the list above for example). We'll provide shortly instructions explaining how to proceed.

Feel free to contact us on Slack in you have trouble with the project.

If you're enthusiastic about our project, :star: it to show your support! :heart:

* * *

## License

[Apache License 2.0](https://github.com/OpenMined/PySyft/blob/master/LICENSE)

