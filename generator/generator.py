import os
import re
import csv
import sys
import uuid
import rdflib
import urllib
import getopt
import subprocess
from rdflib.plugins.sparql import prepareQuery
import traceback
from mysql import connector
from concurrent.futures import ThreadPoolExecutor
from .function_dic import *
from .functions import *
from .string_subs import *

try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm

global id_number
id_number = 0
global g_triples 
g_triples = {}
global number_triple
number_triple = 0
global join_table 
join_table = {}
global po_table
po_table = {}
global dic_table
dic_table = {}
global base
base = ""
global duplicate
duplicate = "yes"
global ignore
ignore = "yes"
global blank_message
blank_message = True
global knowledge_graph
knowledge_graph = ""
global general_predicates
general_predicates = {"http://www.w3.org/2000/01/rdf-schema#subClassOf":"",
						"http://www.w3.org/2002/07/owl#sameAs":"",
						"http://www.w3.org/2000/01/rdf-schema#seeAlso":"",
						"http://www.w3.org/2000/01/rdf-schema#subPropertyOf":""}

def hash_maker_array(parent_data, parent_subject, child_object):
	hash_table = {}
	row_headers=[x[0] for x in parent_data.description]
	for row in parent_data:
		element =row[row_headers.index(child_object.parent[0])]
		if type(element) is int:
			element = str(element)
		if row[row_headers.index(child_object.parent[0])] in hash_table:
			if duplicate == "yes":
				if "<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore) + ">" not in hash_table[row[row_headers.index(child_object.parent[0])]]:
					hash_table[element].update({"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore) + ">" : "object"})
			else:
				hash_table[element].update({"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">" : "object"})
			
		else:
			hash_table.update({element : {"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">" : "object"}}) 
	join_table.update({parent_subject.triples_map_id + "_" + child_object.child[0]  : hash_table})

def hash_maker_array_list(parent_data, parent_subject, child_object, r_w):
	hash_table = {}
	global blank_message
	row_headers=[x[0] for x in parent_data.description]
	for row in parent_data:
		if child_list_value_array(child_object.parent,row,row_headers) in hash_table:
			if duplicate == "yes":
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,"object",ignore)
					if value is not None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					if value not in hash_table[child_list_value_array(child_object.parent,row,row_headers)]:
						hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value + ">" : "object"})

				else:
					value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore)
					if value is not None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						if value not in hash_table[child_list_value_array(child_object.parent,row,row_headers)]:
							hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value : "object"})
			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,"object",ignore)
					if value is not None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value : "object"})
				else:
					value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore)
					if value is not None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
								if "." in value:
									value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value : "object"})
			
		else:
			if parent_subject.subject_map.subject_mapping_type == "reference":
				value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,"object",ignore)
				if value is not None:
					if "http" in value and "<" not in value:
						value = "<" + value[1:-1] + ">"
					elif "http" in value and "<" in value:
							value = value[1:-1]
				hash_table.update({child_list_value_array(child_object.parent,row,row_headers):{value : "object"}})
			else:
				value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore)
				if value is not None:
					if parent_subject.subject_map.term_type != None:
						if "BlankNode" in parent_subject.subject_map.term_type:
							if "/" in value:
								value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
								if blank_message:
									print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
									blank_message = False
							else:
								value = "_:" + encode_char(value).replace("%","")
							if "." in value:
								value = value.replace(".","2E")
					else:
						value = "<" + value + ">"
					hash_table.update({child_list_value_array(child_object.parent,row,row_headers) : {value : "object"}}) 
	join_table.update({parent_subject.triples_map_id + "_" + child_list(child_object.child)  : hash_table})

def dictionary_table_update(resource):
	if resource not in dic_table:
		global id_number
		dic_table[resource] = base36encode(id_number)
		id_number += 1

def mapping_parser(mapping_file):

	"""
	(Private function, not accessible from outside this package)
	Takes a mapping file in Turtle (.ttl) or Notation3 (.n3) format and parses it into a list of
	TriplesMap objects (refer to TriplesMap.py file)
	Parameters
	----------
	mapping_file : string
		Path to the mapping file
	Returns
	-------
	A list of TriplesMap objects containing all the parsed rules from the original mapping file
	"""

	mapping_graph = rdflib.Graph()

	try:
		mapping_graph.parse(data=mapping_file, format='n3')
	except Exception as n3_mapping_parse_exception:
		print(n3_mapping_parse_exception)
		print('Could not parse {} as a mapping file'.format(mapping_file))
		print('Aborting...')
		sys.exit(1)

	mapping_query = """
		prefix rr: <http://www.w3.org/ns/r2rml#> 
		prefix rml: <http://semweb.mmlab.be/ns/rml#> 
		prefix ql: <http://semweb.mmlab.be/ns/ql#> 
		prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#>
		prefix fnml: <http://semweb.mmlab.be/ns/fnml#> 
		SELECT DISTINCT *
		WHERE {
	# Subject -------------------------------------------------------------------------
		
			?triples_map_id rml:logicalSource ?_source .
			OPTIONAL{?_source rml:source ?data_source .}
			OPTIONAL {?_source rml:referenceFormulation ?ref_form .}
			OPTIONAL { ?_source rml:iterator ?iterator . }
			OPTIONAL { ?_source rr:tableName ?tablename .}
			OPTIONAL { ?_source rml:query ?query .}
			
			OPTIONAL {?triples_map_id rr:subjectMap ?_subject_map .}
			OPTIONAL {?_subject_map rr:template ?subject_template .}
			OPTIONAL {?_subject_map rml:reference ?subject_reference .}
			OPTIONAL {?_subject_map rr:constant ?subject_constant}
			OPTIONAL { ?_subject_map rr:class ?rdf_class . }
			OPTIONAL { ?_subject_map rr:termType ?termtype . }
			OPTIONAL { ?_subject_map rr:graph ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:constant ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:template ?graph . }
		   	OPTIONAL {?_subject_map fnml:functionValue ?subject_function .}		   
	# Predicate -----------------------------------------------------------------------
			OPTIONAL {
			?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
			
			OPTIONAL {
				?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:constant ?predicate_constant .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:template ?predicate_template .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rml:reference ?predicate_reference .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicate ?predicate_constant_shortcut .
			 }
			
	# Object --------------------------------------------------------------------------
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:constant ?object_constant .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:template ?object_template .
				OPTIONAL {?_object_map rr:termType ?term .}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rml:reference ?object_reference .
				OPTIONAL { ?_object_map rr:language ?language .}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:parentTriplesMap ?object_parent_triples_map .
				OPTIONAL {
					?_object_map rr:joinCondition ?join_condition .
					?join_condition rr:child ?child_value;
								 rr:parent ?parent_value.
				 	OPTIONAL{?parent_value fnml:functionValue ?parent_function.}
				 	OPTIONAL{?child_value fnml:functionValue ?child_function.}
				 	OPTIONAL {?_object_map rr:termType ?term .}
				}
				OPTIONAL {
					?_object_map rr:joinCondition ?join_condition .
					?join_condition rr:child ?child_value;
								 rr:parent ?parent_value;
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:object ?object_constant_shortcut .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL{
				?_predicate_object_map rr:objectMap ?_object_map .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
				?_object_map fnml:functionValue ?function .
				OPTIONAL {?_object_map rr:termType ?term .}
				
			}
			}
			OPTIONAL {
				?_source a d2rq:Database;
  				d2rq:jdbcDSN ?jdbcDSN; 
  				d2rq:jdbcDriver ?jdbcDriver; 
			    d2rq:username ?user;
			    d2rq:password ?password .
			}
			
		} """

	mapping_query_results = mapping_graph.query(mapping_query)
	triples_map_list = []


	for result_triples_map in mapping_query_results:
		triples_map_exists = False
		for triples_map in triples_map_list:
			triples_map_exists = triples_map_exists or (str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id))
		
		subject_map = None
		if result_triples_map.jdbcDSN is not None:
			jdbcDSN = result_triples_map.jdbcDSN
			jdbcDriver = result_triples_map.jdbcDriver
		if not triples_map_exists:
			if result_triples_map.subject_template is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
			elif result_triples_map.subject_reference is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
			elif result_triples_map.subject_constant is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
			elif result_triples_map.subject_function is not None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
				
			mapping_query_prepared = prepareQuery(mapping_query)


			mapping_query_prepared_results = mapping_graph.query(mapping_query_prepared, initBindings={'triples_map_id': result_triples_map.triples_map_id})




			predicate_object_maps_list = []

			function = False
			for result_predicate_object_map in mapping_query_prepared_results:

				if result_predicate_object_map.predicate_constant is not None:
					predicate_map = tm.PredicateMap("constant", str(result_predicate_object_map.predicate_constant), "")
				elif result_predicate_object_map.predicate_constant_shortcut is not None:
					predicate_map = tm.PredicateMap("constant shortcut", str(result_predicate_object_map.predicate_constant_shortcut), "")
				elif result_predicate_object_map.predicate_template is not None:
					template, condition = string_separetion(str(result_predicate_object_map.predicate_template))
					predicate_map = tm.PredicateMap("template", template, condition)
				elif result_predicate_object_map.predicate_reference is not None:
					reference, condition = string_separetion(str(result_predicate_object_map.predicate_reference))
					predicate_map = tm.PredicateMap("reference", reference, condition)
				else:
					predicate_map = tm.PredicateMap("None", "None", "None")

				if "execute" in predicate_map.value:
					function = True

				if result_predicate_object_map.object_constant is not None:
					object_map = tm.ObjectMap("constant", str(result_predicate_object_map.object_constant), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_template is not None:
					object_map = tm.ObjectMap("template", str(result_predicate_object_map.object_template), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_reference is not None:
					object_map = tm.ObjectMap("reference", str(result_predicate_object_map.object_reference), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_parent_triples_map is not None:
					if (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is not None):
						object_map = tm.ObjectMap("parent triples map function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_function), str(result_predicate_object_map.parent_function), result_predicate_object_map.term, result_predicate_object_map.language)
					elif (result_predicate_object_map.child_function is None) and (result_predicate_object_map.parent_function is not None):
						object_map = tm.ObjectMap("parent triples map parent function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_function), str(result_predicate_object_map.parent_value), result_predicate_object_map.term, result_predicate_object_map.language)
					elif (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is None):
						object_map = tm.ObjectMap("parent triples map child function", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_value), str(result_predicate_object_map.parent_function), result_predicate_object_map.term, result_predicate_object_map.language)
					else:
						object_map = tm.ObjectMap("parent triples map", str(result_predicate_object_map.object_parent_triples_map), str(result_predicate_object_map.object_datatype), str(result_predicate_object_map.child_value), str(result_predicate_object_map.parent_value), result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.object_constant_shortcut is not None:
					object_map = tm.ObjectMap("constant shortcut", str(result_predicate_object_map.object_constant_shortcut), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				elif result_predicate_object_map.function is not None:
					object_map = tm.ObjectMap("reference function", str(result_predicate_object_map.function),str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language)
				else:
					object_map = tm.ObjectMap("None", "None", "None", "None", "None", "None", "None")

				predicate_object_maps_list += [tm.PredicateObjectMap(predicate_map, object_map)]

			if function:
				current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), None, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query),function=True)
			else:
				current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), subject_map, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query),function=False)
			triples_map_list += [current_triples_map]

	return triples_map_list

def semantify_mysql(row, row_headers, triples_map, triples_map_list, output_file_descriptor, csv_file, dataset_name, host, port, user, password,dbase):

	"""
	(Private function, not accessible from outside this package)

	Takes a triples-map rule and applies it to each one of the rows of its CSV data
	source

	Parameters
	----------
	triples_map : TriplesMap object
		Mapping rule consisting of a logical source, a subject-map and several predicateObjectMaps
		(refer to the TriplesMap.py file in the triplesmap folder)
	triples_map_list : list of TriplesMap objects
		List of triples-maps parsed from current mapping being used for the semantification of a
		dataset (mainly used to perform rr:joinCondition mappings)
	delimiter : string
		Delimiter value for the CSV or TSV file ("\s" and "\t" respectively)
	output_file_descriptor : file object 
		Descriptor to the output file (refer to the Python 3 documentation)

	Returns
	-------
	An .nt file per each dataset mentioned in the configuration file semantified.
	If the duplicates are asked to be removed in main memory, also returns a -min.nt
	file with the triples sorted and with the duplicates removed.
	"""
	global blank_message
	global knowledge_graph
	triples_map_triples = {}
	generated_triples = {}
	object_list = []
	subject_value = string_substitution_array(triples_map.subject_map.value, "{(.+?)}", row, row_headers, "subject",ignore)
	i = 0

	if triples_map.subject_map.subject_mapping_type == "template":
		if triples_map.subject_map.term_type is None:
			if triples_map.subject_map.condition == "":

				try:
					subject = "<" + subject_value + ">"
				except:
					subject = None

			else:
			#	field, condition = condition_separetor(triples_map.subject_map.condition)
			#	if row[field] == condition:
				try:
					subject = "<" + subject_value  + ">"
				except:
					subject = None
		else:
			if "IRI" in triples_map.subject_map.term_type:
				if triples_map.subject_map.condition == "":

					try:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + encode_char(subject_value) + ">"
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
					except:
						subject = None
				
			elif "BlankNode" in triples_map.subject_map.term_type:
				if triples_map.subject_map.condition == "":

					try:
						if "/" in subject_value:
							subject  = "_:" + encode_char(subject_value.replace("/","2F")).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E")
							if blank_message:
								print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
								blank_message = False
						else:
							subject = "_:" + encode_char(subject_value).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E")  
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						subject = "_:" + subject_value 
					except:
						subject = None
			elif "Literal" in triples_map.subject_map.term_type:
				subject = None
			else:
				if triples_map.subject_map.condition == "":

					try:
						subject = "<" + subject_value + ">"
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						subject = "<" + subject_value + ">"
					except:
						subject = None

	elif triples_map.subject_map.subject_mapping_type == "reference":
		if triples_map.subject_map.condition == "":
			subject_value = string_substitution_array(triples_map.subject_map.value, ".+", row, row_headers, "subject",ignore)
			subject_value = subject_value[1:-1]
			try:
				if " " not in subject_value:
					if "http" not in subject_value:
						subject = "<" + base + subject_value + ">"
					else:
						subject = "<" + subject_value + ">"
				else:
					print("<http://example.com/base/" + subject_value + "> is an invalid URL")
					subject = None 
			except:
				subject = None
			if triples_map.subject_map.term_type == "IRI":
				if " " not in subject_value:
					subject = "<" + subject_value + ">"
				else:
					subject = None

		else:
		#	field, condition = condition_separetor(triples_map.subject_map.condition)
		#	if row[field] == condition:
			try:
				if "http" not in subject_value:
					subject = "<" + base + subject_value + ">"
				else:
					subject = "<" + subject_value + ">"
			except:
				subject = None

	elif "constant" in triples_map.subject_map.subject_mapping_type:
		subject = "<" + subject_value + ">"
	elif "function" in triples_map.subject_map.subject_mapping_type:
		temp_dics = []
		for triples_map_element in triples_map_list:
			if triples_map_element.triples_map_id == triples_map.subject_map.value:
				dic = create_dictionary(triples_map_element)
				current_func = {"output_name":"OUTPUT" + str(i), 
								"inputs":dic["inputs"], 
								"function":dic["executes"],
								"func_par":dic}
				for inputs in dic["inputs"]:
					temp_dic = {}
					if "reference function" in inputs:
						temp_dic = {"inputs":dic["inputs"], 
										"function":dic["executes"],
										"func_par":dic,
										"id":triples_map_element.triples_map_id}
						if inner_function_exists(temp_dic, temp_dics):
							temp_dics.append(temp_dic)
				if temp_dics:
					func = inner_function(row,row_headers,current_func,triples_map_list)
					subject = "<" + encode_char(func) + ">"
				else:
					func = execute_function(row,row_headers,current_func)
					subject = "<" + encode_char(func) + ">"
			else:
				continue
	else:
		if triples_map.subject_map.condition == "":

			try:
				subject =  "\"" + triples_map.subject_map.value + "\""
			except:
				subject = None

		else:
		#	field, condition = condition_separetor(triples_map.subject_map.condition)
		#	if row[field] == condition:
			try:
				subject =  "\"" + triples_map.subject_map.value + "\""
			except:
				subject = None

	if triples_map.subject_map.rdf_class is not None and subject is not None:
		predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
		for rdf_class in triples_map.subject_map.rdf_class:
			if rdf_class is not None:
				obj = "<{}>".format(rdf_class)
				dictionary_table_update(subject)
				dictionary_table_update(obj)
				dictionary_table_update(predicate + "_" + obj)
				rdf_type = subject + " " + predicate + " " + obj +" .\n"
				for graph in triples_map.subject_map.graph:
					if graph is not None and "defaultGraph" not in graph:
						if "{" in graph:	
							rdf_type = rdf_type[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + "> .\n"
							dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
						else:
							rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
							dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						if dic_table[predicate + "_" + obj] not in g_triples:
							knowledge_graph += rdf_type
							g_triples.update({dic_table[predicate  + "_" + obj ] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
						elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + obj]]:
							knowledge_graph += rdf_type
							g_triples[dic_table[predicate + "_" + obj]].update({dic_table[subject] + "_" + dic_table[obj] : ""})
					else:
						knowledge_graph += rdf_type

	for predicate_object_map in triples_map.predicate_object_maps_list:
		if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
			predicate = "<" + predicate_object_map.predicate_map.value + ">"
		elif predicate_object_map.predicate_map.mapping_type == "template":
			if predicate_object_map.predicate_map.condition != "":
				try:
					predicate = "<" + string_substitution_array(predicate_object_map.predicate_map.value, "{(.+?)}", row, row_headers, "predicate",ignore) + ">"
				except:
					predicate = None
			else:
				try:
					predicate = "<" + string_substitution_array(predicate_object_map.predicate_map.value, "{(.+?)}", row, row_headers, "predicate",ignore) + ">"
				except:
					predicate = None
		elif predicate_object_map.predicate_map.mapping_type == "reference":
			predicate = string_substitution_array(predicate_object_map.predicate_map.value, ".+", row, row_headers, "predicate",ignore)
			predicate = "<" + predicate[1:-1] + ">"
		else:
			predicate = None

		if predicate_object_map.object_map.mapping_type == "constant" or predicate_object_map.object_map.mapping_type == "constant shortcut":
			if "/" in predicate_object_map.object_map.value:
				object = "<" + predicate_object_map.object_map.value + ">"
			else:
				object = "\"" + predicate_object_map.object_map.value + "\""
			if predicate_object_map.object_map.datatype is not None:
				object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
		elif predicate_object_map.object_map.mapping_type == "template":
			try:
				if predicate_object_map.object_map.term is None:
					object = "<" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">"
				elif "IRI" in predicate_object_map.object_map.term:
					object = "<" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">"
				elif "BlankNode" in predicate_object_map.object_map.term:
					object = "_:" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore)
					if "/" in object:
						object  = object.replace("/","2F")
						if blank_message:
							print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
							blank_message = False
					if "." in object:
						object = object.replace(".","2E")
					object = encode_char(object)
				else:
					object = "\"" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore) + "\""
					if predicate_object_map.object_map.datatype is not None:
						object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
					elif predicate_object_map.object_map.language is not None:
						if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
							object += "@es"
						elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
							object += "@en"
						elif len(predicate_object_map.object_map.language) == 2:
							object += "@"+predicate_object_map.object_map.language
			except TypeError:
				object = None
		elif predicate_object_map.object_map.mapping_type == "reference":
			object = string_substitution_array(predicate_object_map.object_map.value, ".+", row, row_headers, "object",ignore)
			if object is not None:
				if "\\" in object[1:-1]:
					object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
				if "'" in object[1:-1]:
					object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
				if "\n" in object:
					object = object.replace("\n","\\n")
				if predicate_object_map.object_map.datatype is not None:
					object += "^^<{}>".format(predicate_object_map.object_map.datatype)
				elif predicate_object_map.object_map.language is not None:
					if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
						object += "@es"
					elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
						object += "@en"
					elif len(predicate_object_map.object_map.language) == 2:
						object += "@"+predicate_object_map.object_map.language
				elif predicate_object_map.object_map.term is not None:
					if "IRI" in predicate_object_map.object_map.term:
						if " " not in object:
							object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
							object = "<" + encode_char(object[1:-1]) + ">"
						else:
							object = None

		elif predicate_object_map.object_map.mapping_type == "parent triples map":
			for triples_map_element in triples_map_list:
				if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
					if (triples_map_element.data_source != triples_map.data_source) or (triples_map_element.tablename != triples_map.tablename):
						if len(predicate_object_map.object_map.child) == 1:
							if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
								database, query_list = translate_sql(triples_map_element)
								db = connector.connect(host = host, port = int(port), user = user, password = password)
								cursor = db.cursor(buffered=True)
								if dbase.lower() != "none":
									cursor.execute("use " + dbase)
								else:
									if database != "None":
										cursor.execute("use " + database)
								if triples_map_element.query != "None":
									cursor.execute(triples_map_element.query)
								else:
									for query in query_list:
										cursor.execute(query)
								hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)
							jt = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]
							if row[row_headers.index(predicate_object_map.object_map.child[0])] is not None:
								object_list = jt[row[row_headers.index(predicate_object_map.object_map.child[0])]]
							object = None
						else:
							if (triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
								database, query_list = translate_sql(triples_map_element)
								db = connector.connect(host=host, port=int(port), user=user, password=password)
								cursor = db.cursor(buffered=True)
								if dbase.lower() != "none":
									cursor.execute("use " + dbase)
								else:
									if database != "None":
										cursor.execute("use " + database)
								if triples_map_element.query != "None":
									cursor.execute(triples_map_element.query)
								else:
									for query in query_list:
										temp_query = query.split("FROM")
										parent_list = ""
										for parent in predicate_object_map.object_map.parent:
											parent_list += ", \'" + parent + "\'"
										new_query = temp_query[0] + parent_list + " FROM " + temp_query[1]
										cursor.execute(new_query)
								hash_maker_array_list(cursor, triples_map_element, predicate_object_map.object_map,row_headers)
							if sublist(predicate_object_map.object_map.child,row_headers):
								if child_list_value_array(predicate_object_map.object_map.child,row,row_headers) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
									object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value_array(predicate_object_map.object_map.child,row,row_headers)]
								else:
									object_list = []
							object = None
					else:
						if predicate_object_map.object_map.parent is not None:
							if len(predicate_object_map.object_map.parent) == 1:
								if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
									database, query_list = translate_sql(triples_map_element)
									db = connector.connect(host = host, port = int(port), user = user, password = password)
									cursor = db.cursor(buffered=True)
									if dbase.lower() != "none":
										cursor.execute("use " + dbase)
									else:
										if database != "None":
											cursor.execute("use " + database)
									if triples_map_element.query != "None":
										cursor.execute(triples_map_element.query)
									else:
										for query in query_list:
											temp_query = query.split("FROM")
											parent_list = ""
											for parent in predicate_object_map.object_map.parent:
												parent_list += ", \'" + parent + "\'"
											new_query = temp_query[0] + parent_list + " FROM " + temp_query[1]
											cursor.execute(new_query)
									hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)

							else:
								if (triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
									database, query_list = translate_sql(triples_map_element)
									db = connector.connect(host=host, port=int(port), user=user, password=password)
									cursor = db.cursor(buffered=True)
									if dbase.lower() != "none":
										cursor.execute("use " + dbase)
									else:
										if database != "None":
											cursor.execute("use " + database)
									if triples_map_element.query != "None":
										cursor.execute(triples_map_element.query)
									else:
										for query in query_list:
											temp_query = query.split("FROM")
											parent_list = ""
											for parent in predicate_object_map.object_map.parent:
												parent_list += ", \'" + parent + "\'"
											new_query = temp_query[0] + parent_list + " FROM " + temp_query[1]
											cursor.execute(new_query)
									hash_maker_array_list(cursor, triples_map_element, predicate_object_map.object_map,row_headers)

							if sublist(predicate_object_map.object_map.child,row_headers):
								if child_list_value_array(predicate_object_map.object_map.child,row,row_headers) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
									object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value_array(predicate_object_map.object_map.child,row,row_headers)]
								else:
									object_list = []
							object = None
						else: 
							try:
								database, query_list = translate_sql(triples_map)
								database2, query_list_origin = translate_sql(triples_map_element)
								db = connector.connect(host = host, port = int(port), user = user, password = password)
								cursor = db.cursor(buffered=True)
								if dbase.lower() != "none":
									cursor.execute("use " + dbase)
								else:
									if database != "None":
										cursor.execute("use " + database)
								if triples_map_element.query != "None":
									cursor.execute(triples_map_element.query)
								else:
									for query in query_list:
										for q in query_list_origin:
											query_1 = q.split("FROM")
											query_2 = query.split("SELECT")[1].split("FROM")[0]
											query_new = query_1[0] + " , " + query_2.replace("DISTINCT","") + " FROM " + query_1[1]
											cursor.execute(query_new)
											r_h=[x[0] for x in cursor.description]
											for r in cursor:
												s = string_substitution_array(triples_map.subject_map.value, "{(.+?)}", r, r_h, "subject",ignore)
												if subject_value == s:
													object = "<" + string_substitution_array(triples_map_element.subject_map.value, "{(.+?)}", r, r_h, "object",ignore) + ">"
							except TypeError:
								object = None
					break
				else:
					continue
		elif predicate_object_map.object_map.mapping_type == "reference function":
			if subject is not None:
				temp_dics = []
				for triples_map_element in triples_map_list:
					if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
						dic = create_dictionary(triples_map_element)
						current_func = {"output_name":"OUTPUT" + str(i), 
										"inputs":dic["inputs"], 
										"function":dic["executes"],
										"func_par":dic}
						for inputs in dic["inputs"]:
							temp_dic = {}
							if "reference function" in inputs:
								temp_dic = {"inputs":dic["inputs"], 
												"function":dic["executes"],
												"func_par":dic,
												"id":triples_map_element.triples_map_id}
								if inner_function_exists(temp_dic, temp_dics):
									temp_dics.append(temp_dic)
						if temp_dics:
							func = inner_function(row,row_headers,current_func,triples_map_list)
							if predicate_object_map.object_map.term is not None:
								if "IRI" in predicate_object_map.object_map.term:
									object = "<" + encode_char(func) + ">"
							else:
								if "" != func:
									object = "\"" + func + "\""
								else:
									object = None
						else:
							if predicate_object_map.object_map.term is not None:
								func = execute_function(row,row_headers,current_func)
								if "IRI" in predicate_object_map.object_map.term:
									object = "<" + encode_char(func) + ">"
							else:
								func = execute_function(row,row_headers,current_func)
								if "" != func:
									object = "\"" + func + "\""
								else:
									object = None
					else:
						continue
			else:
				object = None
		else:
			object = None

		if predicate in general_predicates:
			dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
		else:
			dictionary_table_update(predicate)
		if predicate is not None and object is not None and subject is not None:
			dictionary_table_update(subject)
			dictionary_table_update(object)
			for graph in triples_map.subject_map.graph:
				triple = subject + " " + predicate + " " + object + ".\n"
				if graph is not None and "defaultGraph" not in graph:
					if "{" in graph:
						triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
						dictionary_table_update("<" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">")
					else:
						triple = triple[:-2] + " <" + graph + ">.\n"
						dictionary_table_update("<" + graph + ">")
				if duplicate == "yes":
					if predicate in general_predicates:
						if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
							knowledge_graph += triple
							g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
						elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
							knowledge_graph += triple
							g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
					else:
						if dic_table[predicate] not in g_triples:					
							knowledge_graph += triple
							g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
						elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
							knowledge_graph += triple
							g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
				else:
					knowledge_graph += triple
		elif predicate is not None and subject is not None and object_list:
			dictionary_table_update(subject)
			for obj in object_list:
				dictionary_table_update(obj)
				triple = subject + " " + predicate + " " + obj + ".\n"
				for graph in triples_map.subject_map.graph:
					if graph is not None and "defaultGraph" not in graph:
						if "{" in graph:
							triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
							dictionary_table_update("<" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">")
						else:
							triple = triple[:-2] + " <" + graph + ">.\n"
							dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
								knowledge_graph += triple
								g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
							elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
								knowledge_graph += triple
								g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
						else:
							if dic_table[predicate] not in g_triples:
								knowledge_graph += triple
								g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
							elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
								knowledge_graph += triple
								g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
					else:
						knowledge_graph += triple
			object_list = []
		else:
			continue

def generate_data(user,password,host,port,database,tags,start_date,end_date,frecuency):
	mapping = """

			@prefix rr: <http://www.w3.org/ns/r2rml#> .
			@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
			@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
			@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
			@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
			@prefix schema: <http://schema.org/> .
			@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
			@prefix owl: <http://www.w3.org/2002/07/owl#> .
			@prefix fnml: <http://semweb.mmlab.be/ns/fnml#> .
			@prefix fno: <https://w3id.org/function/ontology#> .
			@prefix platoonFun: <ttp://platoon.eu/Pilot3c/function/> .
			@prefix prov: <http://www.w3.org/ns/prov#> .
			@prefix dcterms: <http://purl.org/dc/terms/> .
			@prefix dc: <http://purl.org/dc/elements/1.1/> .
			@prefix vann: <http://purl.org/vocab/vann/> .
			@prefix voaf: <http://purl.org/vocommons/voaf#> .
			@prefix vs: <http://www.w3.org/2003/06/sw-vocab-status/ns#> .
			@prefix cc: <http://creativecommons.org/ns#>.
			@prefix foaf: <http://xmlns.com/foaf/0.1/>.
			@prefix seas: <https://w3id.org/seas/> .
			@prefix time: <http://www.w3.org/2006/time#> .
			@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
			@prefix brick: <https://brickschema.org/schema/1.1/Brick#> .
			@prefix oema: <http://www.purl.org/oema/ontologynetwork#> .
			@prefix s4bldg: <https://w3id.org/def/saref4bldg#> .
			@prefix cim: <http://www.iec.ch/TC57/CIM#> .
			@prefix gn: <https://www.geonames.org/ontology#> .
			@prefix sch: <https://schema.org/> .
			@prefix wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#> .
			@prefix unit: <http://www.qudt.org/2.1/vocab/unit/> .
			@prefix qudt: <http://www.qudt.org/2.1/schema/qudt/> .
			@prefix ontowind: <http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#> .
			@prefix saref: <https://saref.etsi.org/core/> .
			@prefix gn: <https://www.geonames.org/ontology#> .
			@prefix dqv: <http://www.w3.org/ns/dqv#> .
			@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
			@prefix plt: <https://w3id.org/platoon/>.

			@base <https://w3id.org/platoon/> .

			<PLATOON_DB> a d2rq:Database;
			  d2rq:jdbcDSN \"platoon_db\"; 
			  d2rq:jdbcDriver \"com.mysql.cj.jdbc.Driver\";
			  d2rq:username \"root\";
			  d2rq:password \"1234\" .

			<Pilot3c_Mapping1> a rr:TriplesMap;
			      rml:logicalSource [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  \"\"\"
			        SELECT 
			        DateTime, TagName, Description, Value, Quality

			        FROM v_hist_NanoGUNE WHERE 
			        TagName IN (\'NAN_NAN_P02_ELS_EME_PWR_PVP_GRD\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_141\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_141\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_142\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_142\',\'NAN_NAN_P02_CLS_AHU_TEX_PAS_001\',\'NAN_NAN_P02_ELS_PVP_RAD_PVP_UNS\',\'NAN_NAN_P01_ELS_EME_PWR_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_LGH_OFF\',\'NAN_NAN_P01_ELS_EME_PWR_LGH_OFF\',\'NAN_NAN_P02_CLS_HME_PWR_STO_PAS\',\'NAN_NAN_P02_CLD_CME_PWR_PAS_UNS\',\'NAN_NAN_P02_CLS_HME_PWR_AHU_FAC\',\'NAN_NAN_P02_CLS_CME_PWR_AHU_FAC\',\'NAN_NAN_P02_HOT_HME_PWR_AHU_001\',\'NAN_NAN_P02_CLD_CME_PWR_AHU_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_001\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_003\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_001\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_001\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_001\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_003\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_001\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_001\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_003\',\'NAN_TOW_P05_CLD_EME_ENG_PUM_014\',\'NAN_TOW_P05_CLD_EME_PWR_PUM_014\',\'NAN_TOW_P05_CLD_EME_V03_CHI_002\',\'NAN_TOW_P05_CLD_EME_V02_CHI_002\',\'NAN_TOW_P05_CLD_EME_V01_CHI_002\',\'NAN_TOW_P05_CLD_EME_I02_CHI_002\',\'NAN_TOW_P05_ELS_EME_I01_CHI_002\',\'NAN_TOW_P05_CLS_EME_HEX_PAS_001\',\'NAN_TOW_P05_CLD_CHI_CUR_CHI_002\',\'NAN_TOW_P05_CLD_CHI_STA_CHI_002\',\'NAN_TOW_P05_CLD_CHI_TSU_EVA_002\',\'NAN_TOW_P05_CLD_CHI_TRE_EVA_002\',\'NAN_TOW_P05_CLD_CME_MFR_CHI_001\',\'NAN_TOW_P05_CLD_EME_PWR_CHI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_001\') AND 
			        DateTime > \'2022-05-01 00:00\' AND DateTime <= \'2022-05-02 0:00\'
			        and wwRetrievalMode=\'cyclic\' AND wwResolution=3600000 AND wwQualityRule=\'Extended\'
			      \"\"\"
			    ];
			    rr:subjectMap [
			      rr:template \"https://w3id.org/platoon/Pilot3c/{DateTime}-{TagName}\";
			    ];
			    
			    rr:predicateObjectMap [
			      rr:predicate time:inXSDDateTime;
			      rr:objectMap [
			        rml:reference \"DateTime\";
			        rr:datatype xsd:dateTime 
			      ]
			    ];

			    rr:predicateObjectMap [
			      rr:predicate seas:building;
			      rr:objectMap <BuildingExtraction>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate s4bldg:floor;
			      rr:objectMap <FloorExtraction>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate seas:system;
			      rr:objectMap <SystemExtraction>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate saref:device ;
			      rr:objectMap <DeviceExtraction1> 
			    ];

			    rr:predicateObjectMap [
			      rr:predicate seas:isPropertyOf ; 
			      rr:objectMap <PropertyExtraction>
			    ];
			 
			    rr:predicateObjectMap [
			      rr:predicate saref:device ;
			      rr:objectMap <DeviceExtraction2>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate seas:evaluatedSimpleValue;
			      rr:objectMap [
			        rml:reference  \"Value\";
			        rr:datatype xsd:decimal 
			      ]
			    ];

			    rr:predicateObjectMap [
			      rr:predicate dqv:value;
			      rr:objectMap [
			        rml:reference  \"Quality\";
			        rr:datatype xsd:decimal  
			      ]
			    ].

			<Pilot3c_Mapping2> a rr:TriplesMap;
			      rml:logicalSource [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  \"\"\"
			        SELECT 
			        DateTime, TagName, Description, Value, Quality

			        FROM v_hist_NanoGUNE WHERE 
			        TagName IN (\'NAN_NAN_P02_ELS_EME_PWR_PVP_GRD\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_141\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_141\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_142\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_142\',\'NAN_NAN_P02_CLS_AHU_TEX_PAS_001\',\'NAN_NAN_P02_ELS_PVP_RAD_PVP_UNS\',\'NAN_NAN_P01_ELS_EME_PWR_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_LGH_OFF\',\'NAN_NAN_P01_ELS_EME_PWR_LGH_OFF\',\'NAN_NAN_P02_CLS_HME_PWR_STO_PAS\',\'NAN_NAN_P02_CLD_CME_PWR_PAS_UNS\',\'NAN_NAN_P02_CLS_HME_PWR_AHU_FAC\',\'NAN_NAN_P02_CLS_CME_PWR_AHU_FAC\',\'NAN_NAN_P02_HOT_HME_PWR_AHU_001\',\'NAN_NAN_P02_CLD_CME_PWR_AHU_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_001\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_003\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_001\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_001\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_001\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_003\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_001\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_001\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_003\',\'NAN_TOW_P05_CLD_EME_ENG_PUM_014\',\'NAN_TOW_P05_CLD_EME_PWR_PUM_014\',\'NAN_TOW_P05_CLD_EME_V03_CHI_002\',\'NAN_TOW_P05_CLD_EME_V02_CHI_002\',\'NAN_TOW_P05_CLD_EME_V01_CHI_002\',\'NAN_TOW_P05_CLD_EME_I02_CHI_002\',\'NAN_TOW_P05_ELS_EME_I01_CHI_002\',\'NAN_TOW_P05_CLS_EME_HEX_PAS_001\',\'NAN_TOW_P05_CLD_CHI_CUR_CHI_002\',\'NAN_TOW_P05_CLD_CHI_STA_CHI_002\',\'NAN_TOW_P05_CLD_CHI_TSU_EVA_002\',\'NAN_TOW_P05_CLD_CHI_TRE_EVA_002\',\'NAN_TOW_P05_CLD_CME_MFR_CHI_001\',\'NAN_TOW_P05_CLD_EME_PWR_CHI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_001\') AND 
			        DateTime > \'2022-05-01 00:00\' AND DateTime <= \'2022-05-02 0:00\'
			        and wwRetrievalMode=\'cyclic\' AND wwResolution=3600000 AND wwQualityRule=\'Extended\'
			      \"\"\"
			    ];
			    rr:subjectMap <DeviceExtraction2>;
			   rr:predicateObjectMap [
			      rr:predicate rdf:type ;
			      rr:objectMap <DeviceExtraction3>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate dc:description ;
			      rr:objectMap [
			        rml:reference  \"Description\";
			        rr:datatype xsd:string ;
			      ]
			    ].
			 
			<BuildingExtraction>
				  fnml:functionValue [
				    rml:logicalSource
				    [
				  rml:source <PLATOON_DB>;
				  rr:sqlVersion rr:SQL2008;
				  rml:query  \"\"\"
				    SELECT 
				    DateTime, TagName, Description, Value, Quality

				    SELECT 
			        DateTime, TagName, Description, Value, Quality

			        FROM v_hist_NanoGUNE WHERE 
			        TagName IN (\'NAN_NAN_P02_ELS_EME_PWR_PVP_GRD\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_141\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_141\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_142\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_142\',\'NAN_NAN_P02_CLS_AHU_TEX_PAS_001\',\'NAN_NAN_P02_ELS_PVP_RAD_PVP_UNS\',\'NAN_NAN_P01_ELS_EME_PWR_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_LGH_OFF\',\'NAN_NAN_P01_ELS_EME_PWR_LGH_OFF\',\'NAN_NAN_P02_CLS_HME_PWR_STO_PAS\',\'NAN_NAN_P02_CLD_CME_PWR_PAS_UNS\',\'NAN_NAN_P02_CLS_HME_PWR_AHU_FAC\',\'NAN_NAN_P02_CLS_CME_PWR_AHU_FAC\',\'NAN_NAN_P02_HOT_HME_PWR_AHU_001\',\'NAN_NAN_P02_CLD_CME_PWR_AHU_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_001\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_003\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_001\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_001\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_001\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_003\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_001\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_001\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_003\',\'NAN_TOW_P05_CLD_EME_ENG_PUM_014\',\'NAN_TOW_P05_CLD_EME_PWR_PUM_014\',\'NAN_TOW_P05_CLD_EME_V03_CHI_002\',\'NAN_TOW_P05_CLD_EME_V02_CHI_002\',\'NAN_TOW_P05_CLD_EME_V01_CHI_002\',\'NAN_TOW_P05_CLD_EME_I02_CHI_002\',\'NAN_TOW_P05_ELS_EME_I01_CHI_002\',\'NAN_TOW_P05_CLS_EME_HEX_PAS_001\',\'NAN_TOW_P05_CLD_CHI_CUR_CHI_002\',\'NAN_TOW_P05_CLD_CHI_STA_CHI_002\',\'NAN_TOW_P05_CLD_CHI_TSU_EVA_002\',\'NAN_TOW_P05_CLD_CHI_TRE_EVA_002\',\'NAN_TOW_P05_CLD_CME_MFR_CHI_001\',\'NAN_TOW_P05_CLD_EME_PWR_CHI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_001\') AND 
			        DateTime > \'2022-05-01 00:00\' AND DateTime <= \'2022-05-02 0:00\'
			        and wwRetrievalMode=\'cyclic\' AND wwResolution=3600000 AND wwQualityRule=\'Extended\'
				  \"\"\"
				];
				    rr:predicateObjectMap [
				        rr:predicate fno:executes ;
				        rr:objectMap [ 
				            rr:constant platoonFun:BuildingExtraction 
				        ]
				    ]; 
				    rr:predicateObjectMap [
				        rr:predicate platoonFun:buildingTag;
				        rr:objectMap [ 
				            rml:reference \"TagName\" 
				        ];
				    ];  
				].

			<FloorExtraction>
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  \"\"\"
			        SELECT 
			        DateTime, TagName, Description, Value, Quality

			        FROM v_hist_NanoGUNE WHERE 
			        TagName IN (\'NAN_NAN_P02_ELS_EME_PWR_PVP_GRD\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_141\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_141\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_142\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_142\',\'NAN_NAN_P02_CLS_AHU_TEX_PAS_001\',\'NAN_NAN_P02_ELS_PVP_RAD_PVP_UNS\',\'NAN_NAN_P01_ELS_EME_PWR_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_LGH_OFF\',\'NAN_NAN_P01_ELS_EME_PWR_LGH_OFF\',\'NAN_NAN_P02_CLS_HME_PWR_STO_PAS\',\'NAN_NAN_P02_CLD_CME_PWR_PAS_UNS\',\'NAN_NAN_P02_CLS_HME_PWR_AHU_FAC\',\'NAN_NAN_P02_CLS_CME_PWR_AHU_FAC\',\'NAN_NAN_P02_HOT_HME_PWR_AHU_001\',\'NAN_NAN_P02_CLD_CME_PWR_AHU_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_001\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_003\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_001\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_001\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_001\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_003\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_001\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_001\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_003\',\'NAN_TOW_P05_CLD_EME_ENG_PUM_014\',\'NAN_TOW_P05_CLD_EME_PWR_PUM_014\',\'NAN_TOW_P05_CLD_EME_V03_CHI_002\',\'NAN_TOW_P05_CLD_EME_V02_CHI_002\',\'NAN_TOW_P05_CLD_EME_V01_CHI_002\',\'NAN_TOW_P05_CLD_EME_I02_CHI_002\',\'NAN_TOW_P05_ELS_EME_I01_CHI_002\',\'NAN_TOW_P05_CLS_EME_HEX_PAS_001\',\'NAN_TOW_P05_CLD_CHI_CUR_CHI_002\',\'NAN_TOW_P05_CLD_CHI_STA_CHI_002\',\'NAN_TOW_P05_CLD_CHI_TSU_EVA_002\',\'NAN_TOW_P05_CLD_CHI_TRE_EVA_002\',\'NAN_TOW_P05_CLD_CME_MFR_CHI_001\',\'NAN_TOW_P05_CLD_EME_PWR_CHI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_001\') AND 
			        DateTime > \'2022-05-01 00:00\' AND DateTime <= \'2022-05-02 0:00\'
			        and wwRetrievalMode=\'cyclic\' AND wwResolution=3600000 AND wwQualityRule=\'Extended\'
			      \"\"\"
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:FloorExtraction 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:floorTag;
			            rr:objectMap [ 
			                rml:reference \"TagName\" 
			            ]; 
			        ];                        
			    ].
			<SystemExtraction>
			rr:termType rr:IRI;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  \"\"\"
			        SELECT 
			        DateTime, TagName, Description, Value, Quality

			        FROM v_hist_NanoGUNE WHERE 
			        TagName IN (\'NAN_NAN_P02_ELS_EME_PWR_PVP_GRD\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_141\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_141\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_142\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_142\',\'NAN_NAN_P02_CLS_AHU_TEX_PAS_001\',\'NAN_NAN_P02_ELS_PVP_RAD_PVP_UNS\',\'NAN_NAN_P01_ELS_EME_PWR_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_LGH_OFF\',\'NAN_NAN_P01_ELS_EME_PWR_LGH_OFF\',\'NAN_NAN_P02_CLS_HME_PWR_STO_PAS\',\'NAN_NAN_P02_CLD_CME_PWR_PAS_UNS\',\'NAN_NAN_P02_CLS_HME_PWR_AHU_FAC\',\'NAN_NAN_P02_CLS_CME_PWR_AHU_FAC\',\'NAN_NAN_P02_HOT_HME_PWR_AHU_001\',\'NAN_NAN_P02_CLD_CME_PWR_AHU_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_001\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_003\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_001\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_001\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_001\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_003\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_001\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_001\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_003\',\'NAN_TOW_P05_CLD_EME_ENG_PUM_014\',\'NAN_TOW_P05_CLD_EME_PWR_PUM_014\',\'NAN_TOW_P05_CLD_EME_V03_CHI_002\',\'NAN_TOW_P05_CLD_EME_V02_CHI_002\',\'NAN_TOW_P05_CLD_EME_V01_CHI_002\',\'NAN_TOW_P05_CLD_EME_I02_CHI_002\',\'NAN_TOW_P05_ELS_EME_I01_CHI_002\',\'NAN_TOW_P05_CLS_EME_HEX_PAS_001\',\'NAN_TOW_P05_CLD_CHI_CUR_CHI_002\',\'NAN_TOW_P05_CLD_CHI_STA_CHI_002\',\'NAN_TOW_P05_CLD_CHI_TSU_EVA_002\',\'NAN_TOW_P05_CLD_CHI_TRE_EVA_002\',\'NAN_TOW_P05_CLD_CME_MFR_CHI_001\',\'NAN_TOW_P05_CLD_EME_PWR_CHI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_001\') AND 
			        DateTime > \'2022-05-01 00:00\' AND DateTime <= \'2022-05-02 0:00\'
			        and wwRetrievalMode=\'cyclic\' AND wwResolution=3600000 AND wwQualityRule=\'Extended\'
			      \"\"\"
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:SystemExtraction 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:systemTag;
			            rr:objectMap [ 
			                rml:reference \"TagName\" 
			            ];    
			        ];                     
			    ].
			<DeviceExtraction1>
			rr:termType rr:IRI;
			 fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  \"\"\"
			        SELECT 
			        DateTime, TagName, Description, Value, Quality

			        FROM v_hist_NanoGUNE WHERE 
			        TagName IN (\'NAN_NAN_P02_ELS_EME_PWR_PVP_GRD\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_141\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_141\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_142\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_142\',\'NAN_NAN_P02_CLS_AHU_TEX_PAS_001\',\'NAN_NAN_P02_ELS_PVP_RAD_PVP_UNS\',\'NAN_NAN_P01_ELS_EME_PWR_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_LGH_OFF\',\'NAN_NAN_P01_ELS_EME_PWR_LGH_OFF\',\'NAN_NAN_P02_CLS_HME_PWR_STO_PAS\',\'NAN_NAN_P02_CLD_CME_PWR_PAS_UNS\',\'NAN_NAN_P02_CLS_HME_PWR_AHU_FAC\',\'NAN_NAN_P02_CLS_CME_PWR_AHU_FAC\',\'NAN_NAN_P02_HOT_HME_PWR_AHU_001\',\'NAN_NAN_P02_CLD_CME_PWR_AHU_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_001\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_003\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_001\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_001\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_001\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_003\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_001\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_001\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_003\',\'NAN_TOW_P05_CLD_EME_ENG_PUM_014\',\'NAN_TOW_P05_CLD_EME_PWR_PUM_014\',\'NAN_TOW_P05_CLD_EME_V03_CHI_002\',\'NAN_TOW_P05_CLD_EME_V02_CHI_002\',\'NAN_TOW_P05_CLD_EME_V01_CHI_002\',\'NAN_TOW_P05_CLD_EME_I02_CHI_002\',\'NAN_TOW_P05_ELS_EME_I01_CHI_002\',\'NAN_TOW_P05_CLS_EME_HEX_PAS_001\',\'NAN_TOW_P05_CLD_CHI_CUR_CHI_002\',\'NAN_TOW_P05_CLD_CHI_STA_CHI_002\',\'NAN_TOW_P05_CLD_CHI_TSU_EVA_002\',\'NAN_TOW_P05_CLD_CHI_TRE_EVA_002\',\'NAN_TOW_P05_CLD_CME_MFR_CHI_001\',\'NAN_TOW_P05_CLD_EME_PWR_CHI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_001\') AND 
			        DateTime > \'2022-05-01 00:00\' AND DateTime <= \'2022-05-02 0:00\'
			        and wwRetrievalMode=\'cyclic\' AND wwResolution=3600000 AND wwQualityRule=\'Extended\'
			      \"\"\"
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DeviceExtraction1 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:deviceTag;
			            rr:objectMap [ 
			                rml:reference \"TagName\" 
			            ];   
			        ];                      
			    ].
			<PropertyExtraction>
			rr:termType rr:IRI;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  \"\"\"
			        SELECT 
			        DateTime, TagName, Description, Value, Quality

			        FROM v_hist_NanoGUNE WHERE 
			        TagName IN (\'NAN_NAN_P02_ELS_EME_PWR_PVP_GRD\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_141\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_141\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_142\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_142\',\'NAN_NAN_P02_CLS_AHU_TEX_PAS_001\',\'NAN_NAN_P02_ELS_PVP_RAD_PVP_UNS\',\'NAN_NAN_P01_ELS_EME_PWR_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_LGH_OFF\',\'NAN_NAN_P01_ELS_EME_PWR_LGH_OFF\',\'NAN_NAN_P02_CLS_HME_PWR_STO_PAS\',\'NAN_NAN_P02_CLD_CME_PWR_PAS_UNS\',\'NAN_NAN_P02_CLS_HME_PWR_AHU_FAC\',\'NAN_NAN_P02_CLS_CME_PWR_AHU_FAC\',\'NAN_NAN_P02_HOT_HME_PWR_AHU_001\',\'NAN_NAN_P02_CLD_CME_PWR_AHU_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_001\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_003\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_001\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_001\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_001\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_003\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_001\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_001\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_003\',\'NAN_TOW_P05_CLD_EME_ENG_PUM_014\',\'NAN_TOW_P05_CLD_EME_PWR_PUM_014\',\'NAN_TOW_P05_CLD_EME_V03_CHI_002\',\'NAN_TOW_P05_CLD_EME_V02_CHI_002\',\'NAN_TOW_P05_CLD_EME_V01_CHI_002\',\'NAN_TOW_P05_CLD_EME_I02_CHI_002\',\'NAN_TOW_P05_ELS_EME_I01_CHI_002\',\'NAN_TOW_P05_CLS_EME_HEX_PAS_001\',\'NAN_TOW_P05_CLD_CHI_CUR_CHI_002\',\'NAN_TOW_P05_CLD_CHI_STA_CHI_002\',\'NAN_TOW_P05_CLD_CHI_TSU_EVA_002\',\'NAN_TOW_P05_CLD_CHI_TRE_EVA_002\',\'NAN_TOW_P05_CLD_CME_MFR_CHI_001\',\'NAN_TOW_P05_CLD_EME_PWR_CHI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_001\') AND 
			        DateTime > \'2022-05-01 00:00\' AND DateTime <= \'2022-05-02 0:00\'
			        and wwRetrievalMode=\'cyclic\' AND wwResolution=3600000 AND wwQualityRule=\'Extended\'
			      \"\"\"
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:PropertyExtraction 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:propertyTag;
			            rr:objectMap [ 
			                rml:reference \"TagName\" 
			            ];
			        ];                         
			    ].

			<DeviceExtraction2>
			rr:termType rr:IRI;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  \"\"\"
			        SELECT 
			        DateTime, TagName, Description, Value, Quality

			        FROM v_hist_NanoGUNE WHERE 
			        TagName IN (\'NAN_NAN_P02_ELS_EME_PWR_PVP_GRD\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_141\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_141\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_142\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_142\',\'NAN_NAN_P02_CLS_AHU_TEX_PAS_001\',\'NAN_NAN_P02_ELS_PVP_RAD_PVP_UNS\',\'NAN_NAN_P01_ELS_EME_PWR_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_LGH_OFF\',\'NAN_NAN_P01_ELS_EME_PWR_LGH_OFF\',\'NAN_NAN_P02_CLS_HME_PWR_STO_PAS\',\'NAN_NAN_P02_CLD_CME_PWR_PAS_UNS\',\'NAN_NAN_P02_CLS_HME_PWR_AHU_FAC\',\'NAN_NAN_P02_CLS_CME_PWR_AHU_FAC\',\'NAN_NAN_P02_HOT_HME_PWR_AHU_001\',\'NAN_NAN_P02_CLD_CME_PWR_AHU_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_001\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_003\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_001\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_001\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_001\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_003\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_001\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_001\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_003\',\'NAN_TOW_P05_CLD_EME_ENG_PUM_014\',\'NAN_TOW_P05_CLD_EME_PWR_PUM_014\',\'NAN_TOW_P05_CLD_EME_V03_CHI_002\',\'NAN_TOW_P05_CLD_EME_V02_CHI_002\',\'NAN_TOW_P05_CLD_EME_V01_CHI_002\',\'NAN_TOW_P05_CLD_EME_I02_CHI_002\',\'NAN_TOW_P05_ELS_EME_I01_CHI_002\',\'NAN_TOW_P05_CLS_EME_HEX_PAS_001\',\'NAN_TOW_P05_CLD_CHI_CUR_CHI_002\',\'NAN_TOW_P05_CLD_CHI_STA_CHI_002\',\'NAN_TOW_P05_CLD_CHI_TSU_EVA_002\',\'NAN_TOW_P05_CLD_CHI_TRE_EVA_002\',\'NAN_TOW_P05_CLD_CME_MFR_CHI_001\',\'NAN_TOW_P05_CLD_EME_PWR_CHI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_001\') AND 
			        DateTime > \'2022-05-01 00:00\' AND DateTime <= \'2022-05-02 0:00\'
			        and wwRetrievalMode=\'cyclic\' AND wwResolution=3600000 AND wwQualityRule=\'Extended\'
			      \"\"\"
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DeviceExtraction2 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:deviceTag;
			            rr:objectMap [ 
			                rml:reference \"TagName\" 
			            ]; 
			        ];                        
			    ].


			<DeviceExtraction3>
			rr:termType rr:IRI;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  \"\"\"
			        SELECT 
			        DateTime, TagName, Description, Value, Quality

			        FROM v_hist_NanoGUNE WHERE 
			        TagName IN (\'NAN_NAN_P02_ELS_EME_PWR_PVP_GRD\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_141\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_141\',\'NAN_TOW_P05_CLD_PUM_FRQ_TPU_142\',\'NAN_TOW_P05_CLD_PUM_MFR_TPU_142\',\'NAN_NAN_P02_CLS_AHU_TEX_PAS_001\',\'NAN_NAN_P02_ELS_PVP_RAD_PVP_UNS\',\'NAN_NAN_P01_ELS_EME_PWR_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_OFF_GEN\',\'NAN_NAN_P01_ELS_EME_ENG_LGH_OFF\',\'NAN_NAN_P01_ELS_EME_PWR_LGH_OFF\',\'NAN_NAN_P02_CLS_HME_PWR_STO_PAS\',\'NAN_NAN_P02_CLD_CME_PWR_PAS_UNS\',\'NAN_NAN_P02_CLS_HME_PWR_AHU_FAC\',\'NAN_NAN_P02_CLS_CME_PWR_AHU_FAC\',\'NAN_NAN_P02_HOT_HME_PWR_AHU_001\',\'NAN_NAN_P02_CLD_CME_PWR_AHU_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_001\',\'NAN_TOW_P05_HOT_HME_PWR_BOI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_001\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_002\',\'NAN_NAN_P01_CLS_FAC_TAM_HAL_003\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_001\',\'NAN_NAN_P01_CLS_FAC_TAM_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_001\',\'NAN_NAN_P01_HOT_FAC_SPH_KIT_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_001\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_002\',\'NAN_NAN_P01_HOT_FAC_SPH_HAL_003\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_001\',\'NAN_NAN_P01_CLD_FAC_SPC_KIT_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_001\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_002\',\'NAN_NAN_P01_CLD_FAC_SPC_HAL_003\',\'NAN_TOW_P05_CLD_EME_ENG_PUM_014\',\'NAN_TOW_P05_CLD_EME_PWR_PUM_014\',\'NAN_TOW_P05_CLD_EME_V03_CHI_002\',\'NAN_TOW_P05_CLD_EME_V02_CHI_002\',\'NAN_TOW_P05_CLD_EME_V01_CHI_002\',\'NAN_TOW_P05_CLD_EME_I02_CHI_002\',\'NAN_TOW_P05_ELS_EME_I01_CHI_002\',\'NAN_TOW_P05_CLS_EME_HEX_PAS_001\',\'NAN_TOW_P05_CLD_CHI_CUR_CHI_002\',\'NAN_TOW_P05_CLD_CHI_STA_CHI_002\',\'NAN_TOW_P05_CLD_CHI_TSU_EVA_002\',\'NAN_TOW_P05_CLD_CHI_TRE_EVA_002\',\'NAN_TOW_P05_CLD_CME_MFR_CHI_001\',\'NAN_TOW_P05_CLD_EME_PWR_CHI_002\',\'NAN_TOW_P05_CLD_CME_PWR_CHI_001\') AND 
			        DateTime > \'2022-05-01 00:00\' AND DateTime <= \'2022-05-02 0:00\'
			        and wwRetrievalMode=\'cyclic\' AND wwResolution=3600000 AND wwQualityRule=\'Extended\'
			      \"\"\"
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DeviceExtraction3 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:deviceTag;
			            rr:objectMap [ 
			                rml:reference \"TagName\" 
			            ]; 
			        ];                        
			    ].

	"""

	db = connector.connect(host = host, port = int(port), user = user, password = password)
	cursor = db.cursor(buffered=True)
	cursor.execute("use " + database)

	print("Beginning Data Generation Process.")

	with ThreadPoolExecutor(max_workers=10) as executor:
		triples_map_list = mapping_parser(mapping)
		for triples_map in triples_map_list:
			if triples_map.subject_map != None:
				cursor.execute(triples_map.query)
				row_headers=[x[0] for x in cursor.description]
				for row in cursor:
					executor.submit(semantify_mysql, row, row_headers, triples_map, triples_map_list, "", "", "", host, int(port), user, password,database).result()
	global knowledge_graph
	print(knowledge_graph)
	print("The Process has ended.")												