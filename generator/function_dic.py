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