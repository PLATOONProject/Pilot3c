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

functions_pool = {"tolower":"","BuildingExtraction":"","FloorExtraction":"","SystemExtraction":"","PropertyExtraction":"","DeviceExtraction1":"","DeviceExtraction2":"","DeviceExtraction3":""}


## Define your functions here following examples below, the column "names" from the csv files 
## that you aim to use as the input parameters of functions are only required to be provided 
## as the keys of "global_dic"
def tolower(): 
    return str(global_dic["value"]).lower()

def BuildingExtraction():
    tokens = str(global_dic["buildingTag"]).split("_")
    return tokens[0] + "_" + tokens[1]

def FloorExtraction():
    return str(global_dic["floorTag"]).split("_")[2]

def SystemExtraction():
    dic = {
            "CLD":"plt:coolingsystem",
            "CLS":"plt:Ventilationsystem",
            "ELS":"seas:electricpowersystem",
            "HOT":"plt:heatingsystem",
            "OTH":"seas:System"
            }
    for key in dic:
        if key in str(global_dic["systemTag"]):
            return dic[key]

def PropertyExtraction():
    dic = {
            "ALM":"Alarm/plt:Alert",
            "CLK":"time:hours",
            "CUR":"seas:ElectricalCurrent",
            "ENG":"seas:electricEnergyProperty",
            "FRQ":"seas:frequencyproperty",
            "HEX":"saref:Humidity",
            "I01":"seas:rCurrent",
            "I02":"seas:sCurrent",
            "I03":"seas:tCurrent",
            "MFR":"plt:WaterFlow",
            "PWR":"Power",
            "RAD":"seas:DirectRadiation",
            "SPC":"plt:coolingairtemperaturesetpoint",
            "SPH":"plt:heatingairtemperaturesetpoint",
            "SPO":"saref:state",
            "STA":"saref:state",
            "TAM":"plt:indoorAirTemperature",
            "TEX":"plt:OutdoorAirTemperature",
            "TRE":"temperature(return)/saref:temperature",
            "TSU":"temperature(supply)/saref:temperature",
            "V01":"seas:RSVoltage",
            "V02":"seas:STVoltage",
            "V03":"seas:TRVoltage",
            "TMP":"saref:Temperature"
            }
    for key in dic:
        if key in str(global_dic["propertyTag"]):
            return dic[key]

def DeviceExtraction1():
    dic = {
            "AHU":"brick:AHU",
            "CHI":"brick:chiller",
            "CME":"saref:EnergyMeter(Cold)",
            "EME":"seas:ElectricityMeter",
            "FAC":"brick:Fan Coil Unit",
            "HME":"saref:EnergyMeter(Heat)",
            "PUM":"plt:Pump",
            "PVP":"seas:SolarPanel",
            "OTH":"saref:device"
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
            "AHU":"brick:AHU",
            "BOI":"brick:boiler",
            "CHI":"brick:chiller",
            "EVA":"Plt:ChillerEvaporator",
            "HAL":"bot:zone",
            "KIT":"bot:zone",
            "OFF":"bot:zone",
            "PAS":"plt:ventilationsystem",
            "PLG":"brick:Plug",
            "PUM":"plt:Pump",
            "PVP":"seas:SolarPanel",
            "TPU":"plt:pump",
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
            "FAC":"brick:Fan Coil Unit",
            "GEN":"seas:ElectricPowerProducer",
            "GRD":"eas:ElectricPowerSystem",
            "PAS":"plt:VentilationSystem",
            "UNS":"seas:system"
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