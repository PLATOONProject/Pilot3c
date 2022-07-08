import re
import csv
import sys
import os
from .string_subs import *
################################################################################################
############################ Static (Do NOT change this code) ##################################
################################################################################################

import requests

global global_dic
global_dic = {}
global functions_pool

#####################################################################################################
########### ADD THE IMPLEMENTATION OF YOUR FUNCTIONS HERE FOLLOWING THE EXAMPLES ####################
#####################################################################################################

functions_pool = {"tolower":"","BuildingExtraction":"","FloorExtraction":"","SystemExtraction":"",
                    "PropertyExtraction":"","DeviceExtraction1":"","DeviceExtraction2":"",
                    "DeviceExtraction3":"","DateTimeTransformation":"","LocationExtraction":""}


## Define your functions here following examples below, the column "names" from the csv files 
## that you aim to use as the input parameters of functions are only required to be provided 
## as the keys of "global_dic"
def tolower(): 
    return str(global_dic["value"]).lower()

def DateTimeTransformation():
    if ".0000000" in global_dic["dateTime"]:
        return global_dic["dateTime"] + "+2:00"
    else:
        return global_dic["dateTime"] + ".0000000+2:00"

def BuildingExtraction():
    tokens = str(global_dic["buildingTag"]).split("_")
    return tokens[0] + "_" + tokens[1]

def FloorExtraction():
    return str(global_dic["floorTag"]).split("_")[2]

def LocationExtraction():
    dic = {
            "DON":"Donosti/San Sebastián",
            "ESP":"Spain"
            }
    for key in dic:
        if key in str(global_dic["locationTag"]):
            return dic[key]

def SystemExtraction():
    dic = {
            "CLD":"https://w3id.org/platoon/coolingsystem",
            "CLS":"https://w3id.org/platoon/Ventilationsystem",
            "ELS":"https://w3id.org/seas/electricpowersystem",
            "HOT":"https://w3id.org/platoon/heatingsystem",
            "OTH":"https://w3id.org/seas/System"
            }
    for key in dic:
        if key in str(global_dic["systemTag"]):
            return dic[key]

def PropertyExtraction():
    dic = {
            "ALM":"https://w3id.org/platoon/Alert",
            "CLK":"http://www.w3.org/2006/time#hours",
            "CUR":"https://w3id.org/seas/ElectricalCurrent",
            "ENG":"https://w3id.org/seas/electricEnergyProperty",
            "EUR":"https://schema.org/price",
            "FRQ":"https://w3id.org/seas/frequencyproperty",
            "HEX":"http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#Humidity",
            "I01":"https://w3id.org/seas/rCurrent",
            "I02":"https://w3id.org/seas/sCurrent",
            "I03":"https://w3id.org/seas/tCurrent",
            "MFR":"https://w3id.org/platoon/WaterFlow",
            "PWR":"https://saref.etsi.org/core/Power",
            "RAD":"https://w3id.org/seas/DirectRadiation",
            "SPC":"https://w3id.org/platoon/coolingairtemperaturesetpoint",
            "SPH":"https://w3id.org/platoon/heatingairtemperaturesetpoint",
            "SPO":"http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#state",
            "STA":"http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#state",
            "TAM":"https://w3id.org/platoon/indoorAirTemperature",
            "TEX":"https://w3id.org/platoon/OutdoorAirTemperature",
            "TRE":"http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#temperature",
            "TSU":"http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#temperature",
            "V01":"https://w3id.org/seas/RSVoltage",
            "V02":"https://w3id.org/seas/STVoltage",
            "V03":"https://w3id.org/seas/TRVoltage",
            "TMP":"http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#Temperature"
            }
    for key in dic:
        if key in str(global_dic["propertyTag"]):
            return dic[key]

def DeviceExtraction1():
    dic = {
            "AHU":"https://brickschema.org/schema/1.1/Brick#AHU",
            "CHI":"https://brickschema.org/schema/1.1/Brick#chiller",
            "CME":"http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#EnergyMeter(Cold)",
            "EME":"https://w3id.org/seas/ElectricityMeter",
            "FAC":"https://brickschema.org/schema/1.1/Brick#Fan Coil Unit",
            "HME":"http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#EnergyMeter(Heat)",
            "PUM":"https://w3id.org/platoon/Pump",
            "PVP":"https://w3id.org/seas/SolarPanel",
            "OTH":"http://www.semanticweb.org/ontologies/2011/9/Ontology1318785573683.owl#device"
            }
    for key in dic:
        if key in str(global_dic["deviceTag"]):
            return dic[key]

def DeviceExtraction2():
    tokens = str(global_dic["deviceTag"]).split("_")
    length = len(tokens)
    return "https://w3id.org/platoon/Pilot3c/" + tokens[length-1] + "_" + tokens[length-2]

def DeviceExtraction3():
    dic = {
            "AHU":"https://brickschema.org/schema/1.1/Brick#AHU",
            "BOI":"https://brickschema.org/schema/1.1/Brick#boiler",
            "CHI":"https://brickschema.org/schema/1.1/Brick#chiller",
            "EVA":"https://w3id.org/platoon/ChillerEvaporator",
            "HAL":"https://w3id.org/bot#zone",
            "KIT":"https://w3id.org/bot#zone",
            "OFF":"https://w3id.org/bot#zone",
            "PAS":"https://w3id.org/platoon/ventilationsystem",
            "PLG":"https://brickschema.org/schema/1.1/Brick#Plug",
            "PUM":"https://w3id.org/platoon/Pump",
            "PVP":"https://w3id.org/seas/SolarPanel",
            "TPU":"https://w3id.org/platoon/pump",
            "172":"172",
            "171":"171",
            "142":"142",
            "141":"141",
            "92":"92",
            "91":"91",
            "42":"42",
            "41":"41",
            "14":"14",
            "3":"3",
            "2":"2",
            "1":"1",
            "FAC":"https://brickschema.org/schema/1.1/Brick#Fan Coil Unit",
            "GEN":"https://w3id.org/seas/ElectricPowerProducer",
            "GRD":"https://w3id.org/seas/ElectricPowerSystem",
            "PAS":"https://w3id.org/platoon/VentilationSystem",
            "UNS":"https://w3id.org/seas/system"
    }
    for key in dic:
        if key in str(global_dic["deviceTag"]):
            return dic[key]

################################################################################################
############################ Static (Do NOT change this code) ##################################
################################################################################################

def execute_function(row,header,dic):
    if "#" in dic["function"]:
        func = dic["function"].split("#")[1]
    else:
        func = dic["function"].split("/")[len(dic["function"].split("/"))-1]
    #print(dic)
    if func in functions_pool:
        global global_dic
        global_dic = execution_dic(row,header,dic)
        return eval(func + "()")             
    else:
        print("Invalid function")
        print("Aborting...")
        sys.exit(1)

def execution_dic(row,header,dic):
    output = {}
    for inputs in dic["inputs"]:
        if "constant" not in inputs:
            if "reference" in inputs[1]:
                if isinstance(row,dict):
                    output[inputs[2]] = row[inputs[0]]
                else:
                    output[inputs[2]] = row[header.index(inputs[0])]
            elif "template" in inputs:
                if isinstance(row,dict):
                    output[inputs[2]] = string_substitution(inputs[0], "{(.+?)}", row, "subject", "yes", "None")
                else:
                    output[inputs[2]] = string_substitution_array(inputs[0], "{(.+?)}", row, header, "subject", "yes")
        else:
            output[inputs[2]] = inputs[0]
    return output