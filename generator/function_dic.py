import re
import csv
import sys
import os
import pandas as pd 
from .functions import *
import math

def base36encode(number, alphabet='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
	"""Converts an integer to a base36 string."""


	base36 = ''
	sign = ''

	if number < 0:
		sign = '-'
		number = -number

	if 0 <= number < len(alphabet):
		return sign + alphabet[number]

	while number != 0:
		number, i = divmod(number, len(alphabet))
		base36 = alphabet[i] + base36

	return sign + base36

def sublist(part_list, full_list):
	for part in part_list:
		if part not in full_list:
			return False
	return True

def child_list(childs):
	value = ""
	for child in childs:
		if child not in value:
			value += child + "_"
	return value[:-1]

def child_list_value(childs,row):
	value = ""
	v = []
	for child in childs:
		if child not in v:
			if row[child] != None:
				value += str(row[child]) + "_"
				v.append(child)
	return value[:-1]

def child_list_value_array(childs,row,row_headers):
	value = ""
	v = []
	for child in childs:
		if child not in v:
			if row[row_headers.index(child)] != None:
				value += row[row_headers.index(child)] + "_"
				v.append(child)
	return value[:-1]

def dic_builder(keys,values):
    dic = {}
    for key in keys:
        if "template" == key[1]:
            for string in key[0].split("{"):
                if "}" in string:
                    dic[string.split("}")[0]] = str(values[string.split("}")[0]])
        elif (key[1] != "constant") and ("reference function" not in key[1]):
            dic[key[0]] = values[key[0]]
    return dic

def inner_function_exists(inner_func, inner_functions):
	for inner_function in inner_functions:
		if inner_func["id"] in inner_function["id"]:
			return False
	return True

def inner_function(row,headers,dic,triples_map_list):

	functions = []
	keys = []
	for attr in dic["inputs"]:
		if ("reference function" in attr[1]):
			functions.append(attr[0])
		elif "template" in attr[1]:
			for value in attr[0].split("{"):
				if "}" in value:
					keys.append(value.split("}")[0]) 
		elif "constant" not in attr[1]:
			keys.append(attr[0])
	if functions:
		temp_dics = {}
		for function in functions:
			for tp in triples_map_list:
				if tp.triples_map_id == function:
					temp_dic = create_dictionary(tp)
					current_func = {"inputs":temp_dic["inputs"], 
									"function":temp_dic["executes"],
									"func_par":temp_dic,
									"termType":True}
					temp_dics[function] = current_func
		temp_row = {}
		for dics in temp_dics:
			value = inner_function(row,headers,temp_dics[dics],triples_map_list)
			temp_row[dics] = value
		for key in keys:
			if isinstance(row,dict):
				temp_row[key] = row[key]
			else:
				temp_row[key] = row[header.index(key)]
		return execute_function(temp_row,headers,dic)
	else:
		return execute_function(row,headers,dic)

def string_separetion(string):
	if ("{" in string) and ("[" in string):
		prefix = string.split("{")[0]
		condition = string.split("{")[1].split("}")[0]
		postfix = string.split("{")[1].split("}")[1]
		field = prefix + "*" + postfix
	elif "[" in string:
		return string, string
	else:
		return string, ""
	return string, condition

def condition_separetor(string):
	condition_field = string.split("[")
	field = condition_field[1][:len(condition_field[1])-1].split("=")[0]
	value = condition_field[1][:len(condition_field[1])-1].split("=")[1]
	return field, value

def create_dictionary(triple_map):
	dic = {}
	inputs = []
	for tp in triple_map.predicate_object_maps_list:
		if "#" in tp.predicate_map.value:
			key = tp.predicate_map.value.split("#")[1]
			tp_type = tp.predicate_map.mapping_type
		elif "/" in tp.predicate_map.value:
			key = tp.predicate_map.value.split("/")[len(tp.predicate_map.value.split("/"))-1]
			tp_type = tp.predicate_map.mapping_type
		if "constant" in tp.object_map.mapping_type:
			value = tp.object_map.value
			tp_type = tp.object_map.mapping_type
		if "template" in tp.object_map.mapping_type:
			value = tp.object_map.value
			tp_type = tp.object_map.mapping_type
		elif "executes" in tp.predicate_map.value:
			if "#" in tp.object_map.value:
				value = tp.object_map.value.split("#")[1]
				tp_type = tp.object_map.mapping_type
			elif "/" in tp.object_map.value:
				value = tp.object_map.value.split("/")[len(tp.object_map.value.split("/"))-1]
				tp_type = tp.object_map.mapping_type
		else:
		    value = tp.object_map.value
		    tp_type = tp.object_map.mapping_type

		dic.update({key : value})
		if (key != "executes") and ([value,tp_type,key] not in inputs):
		    inputs.append([value,tp_type,key])

	dic["inputs"] = inputs
	return dic

def forecast_mapping_generation(table,tags,date_saved,start_date,end_date):
	query = "SELECT TagName, Description, DateTimeFuture, DateTimeSaved, Value, "
	query += "\'" + start_date + "\' as startDate, \'" + end_date + "\' as endDate\n"
	query += "FROM " + table + " WHERE\n"
	query += "TagName IN ("
	for tag in tags:
		query += "\'" + tag + "\',"
	query = query[:-1] + ")\n "
	query += " AND DateTimeFuture >= \'" + start_date + "\' AND DateTimeFuture <= \'" + end_date + "\'\n"
	query += " AND DateTimeSaved > \'"+ date_saved + "\'\n"
	query = "\"\"\"\n" + query + "\"\"\"\n"
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
			@prefix dbr: <http://dbpedia.org/resource/>.
			@prefix dbo: <http://dbpedia.org/ontology/>.

			@base <https://w3id.org/platoon/> .

			<PLATOON_DB> a d2rq:Database;
			  d2rq:jdbcDSN \"platoon_db\"; 
			  d2rq:jdbcDriver \"com.mysql.cj.jdbc.Driver\";
			  d2rq:username \"root\";
			  d2rq:password \"1234\" .

		  	<Pilot3c_Spain> a rr:TriplesMap;
			      rml:logicalSource [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query <PLATOON_QUERY>
			    ];
			    rr:subjectMap [
			      rr:template \"https://w3id.org/platoon/Pilot3c/Spain\";
			      rr:class dbo:Location;
			    ];

			    rr:predicateObjectMap [
			      rr:predicate owl:sameAs;
			      rr:objectMap [
			      		rr:template \"https://www.wikidata.org/entity/Q29\";
			      ]; 
			    ];

			    rr:predicateObjectMap [
			      rr:predicate owl:sameAs;
			      rr:objectMap [
			      		rr:constant dbr:Spain;
			      ]; 
			    ].

		    <Pilot3c_Donosti> a rr:TriplesMap;
			      rml:logicalSource [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query <PLATOON_QUERY>
			    ];
			    rr:subjectMap [
			      rr:template \"https://w3id.org/platoon/Pilot3c/Donosti-San%20Sebasti%C3%A1n\";
			      rr:class dbo:Location;
			    ];

			    rr:predicateObjectMap [
			      rr:predicate owl:sameAs;
			      rr:objectMap [
			      		rr:template \"https://www.wikidata.org/entity/Q10313\";
			      ]; 
			    ];

			    rr:predicateObjectMap [
			      rr:predicate owl:sameAs;
			      rr:objectMap [
			      		rr:constant dbr:Donostia-San%20Sebasti%C3%A1n;
			      ]; 
			    ].

			<Pilot3c_Mapping1> a rr:TriplesMap;
			      rml:logicalSource [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query <PLATOON_QUERY>
			    ];
			    rr:subjectMap [
			      rr:template \"https://w3id.org/platoon/Pilot3c/{DateTimeFuture}-{DateTimeSaved}-{TagName}\";
			      rr:class seas:Forecast;
			    ];

			    rr:predicateObjectMap [
			      rr:predicate seas:location;
			      rr:objectMap <LocationExtraction>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate seas:locationResource;
			      rr:objectMap <LocationExtractionResource>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate seas:system;
			      rr:objectMap <SystemExtraction>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate seas:isPropertyOf ; 
			      rr:objectMap <PropertyExtraction>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate time:hasBeginning; 
			      rr:objectMap <startDateTransformation>
			    ];
			 
			    rr:predicateObjectMap [
			      rr:predicate time:hasEnded;
			      rr:objectMap <endDateTransformation>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate time:inXSDDateTime; 
			      rr:objectMap <savedDateTransformation>
			    ];
			 
			    rr:predicateObjectMap [
			      rr:predicate time:inside;
			      rr:objectMap <futureDateTransformation>
			    ];

			    rr:predicateObjectMap [
			      rr:predicate dc:description ;
			      rr:objectMap [
			        rml:reference  \"Description\";
			        rr:datatype xsd:string ;
			      ]
			    ];

			    rr:predicateObjectMap [
			      rr:predicate seas:evaluatedSimpleValue;
			      rr:objectMap [
			        rml:reference  \"Value\";
			        rr:datatype xsd:decimal 
			      ]
			    ].
			 
		    <LocationExtraction>
				  fnml:functionValue [
				    rml:logicalSource
				    [
				  rml:source <PLATOON_DB>;
				  rr:sqlVersion rr:SQL2008;
				  rml:query <PLATOON_QUERY>
				];
				    rr:predicateObjectMap [
				        rr:predicate fno:executes ;
				        rr:objectMap [ 
				            rr:constant platoonFun:LocationExtraction 
				        ]
				    ]; 
				    rr:predicateObjectMap [
				        rr:predicate platoonFun:locationTag;
				        rr:objectMap [ 
				            rml:reference \"TagName\" 
				        ];
				    ];  
				].

			<LocationExtractionResource>
				rr:termType rr:IRI;
				  fnml:functionValue [
				    rml:logicalSource
				    [
				  rml:source <PLATOON_DB>;
				  rr:sqlVersion rr:SQL2008;
				  rml:query <PLATOON_QUERY>
				];
				    rr:predicateObjectMap [
				        rr:predicate fno:executes ;
				        rr:objectMap [ 
				            rr:constant platoonFun:LocationExtractionResource 
				        ]
				    ]; 
				    rr:predicateObjectMap [
				        rr:predicate platoonFun:locationTag;
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
			      rml:query <PLATOON_QUERY>
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
			<PropertyExtraction>
			rr:termType rr:IRI;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query <PLATOON_QUERY>
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

		    <savedDateTransformation>
			rr:datatype xsd:dateTime;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query <PLATOON_QUERY>
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DateTimeTransformation 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:dateTime;
			            rr:objectMap [ 
			                rml:reference \"DateTimeSaved\" 
			            ]; 
			        ];                        
			    ].

		    <futureDateTransformation>
			rr:datatype xsd:dateTime;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query <PLATOON_QUERY>
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DateTimeTransformation 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:dateTime;
			            rr:objectMap [ 
			                rml:reference \"DateTimeFuture\" 
			            ]; 
			        ];                        
			    ].

	    	<startDateTransformation>
			rr:datatype xsd:dateTime;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query <PLATOON_QUERY>
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DateTimeTransformation 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:dateTime;
			            rr:objectMap [ 
			                rml:reference \"startDate\" 
			            ]; 
			        ];                        
			    ].


		    <endDateTransformation>
			rr:datatype xsd:dateTime;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  <PLATOON_QUERY>
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DateTimeTransformation 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:dateTime;
			            rr:objectMap [ 
			                rml:reference \"endDate\" 
			            ]; 
			        ];                        
			    ].

	"""
	return mapping.replace("<PLATOON_QUERY>",query)

def mapping_generation(table,tags,start_date,end_date,resolution):
	query = "SELECT DateTime, TagName, Description, Value, Quality, "
	query += "\'" + start_date + "\' as startDate, \'" + end_date + "\' as endDate\n"
	query += "FROM " + table + " WHERE\n"
	query += "TagName IN ("
	for tag in tags:
		query += "\'" + tag + "\',"
	query = query[:-1] + ")\n "
	query += " AND DateTime > \'" + start_date + "\' AND DateTime <= \'" + end_date + "\'\n"
	query += " AND wwRetrievalMode=\'cyclic\' AND wwResolution="+ str(resolution) + " AND wwQualityRule=\'Extended\'\n"
	query = "\"\"\"\n" + query + "\"\"\"\n"
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
			      rml:query <PLATOON_QUERY>
			    ];
			    rr:subjectMap [
			      rr:template \"https://w3id.org/platoon/Pilot3c/{DateTime}-{TagName}\";
			    ];
			    
			    rr:predicateObjectMap [
			      rr:predicate time:inside;
			      rr:objectMap <DateTimeTransformation>
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
			      rr:predicate time:hasBeginning; 
			      rr:objectMap <startDateTransformation>
			    ];
			 
			    rr:predicateObjectMap [
			      rr:predicate time:hasEnded;
			      rr:objectMap <endDateTransformation>
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
			      rml:query <PLATOON_QUERY>
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
				  rml:query <PLATOON_QUERY>
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
			      rml:query <PLATOON_QUERY>
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
			      rml:query <PLATOON_QUERY>
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
			      rml:query <PLATOON_QUERY>
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
			      rml:query <PLATOON_QUERY>
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
			      rml:query <PLATOON_QUERY>
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
			      rml:query <PLATOON_QUERY>
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

	    	<startDateTransformation>
			rr:datatype xsd:dateTime;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query <PLATOON_QUERY>
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DateTimeTransformation 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:dateTime;
			            rr:objectMap [ 
			                rml:reference \"startDate\" 
			            ]; 
			        ];                        
			    ].


		    <endDateTransformation>
			rr:datatype xsd:dateTime;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  <PLATOON_QUERY>
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DateTimeTransformation 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:dateTime;
			            rr:objectMap [ 
			                rml:reference \"endDate\" 
			            ]; 
			        ];                        
			    ].

		    <DateTimeTransformation>
			rr:datatype xsd:dateTime;
			fnml:functionValue [
			        rml:logicalSource
			        [
			      rml:source <PLATOON_DB>;
			      rr:sqlVersion rr:SQL2008;
			      rml:query  <PLATOON_QUERY>
			    ];
			        rr:predicateObjectMap [
			            rr:predicate fno:executes ;
			            rr:objectMap [ 
			                rr:constant platoonFun:DateTimeTransformation 
			            ]
			        ]; 
			        rr:predicateObjectMap [
			            rr:predicate platoonFun:dateTime;
			            rr:objectMap [ 
			                rml:reference \"DateTime\" 
			            ]; 
			        ];                        
			    ].


	"""
	return mapping.replace("<PLATOON_QUERY>",query)