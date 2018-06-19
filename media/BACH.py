# -*- coding: latin-1 -*-
import re
import datetime
import config
import csv

client = config.client
nivel = ["media"]
subnivel = ["general"]
csvfile = open('BACH GEN F9117G-32.txt', 'rb')
table = csv.reader(csvfile, delimiter ='|')
field_names = table.next()
records = csv.DictReader(csvfile, fieldnames=field_names, delimiter ='|')
#print field_names
#print table.records[0]
r = re.compile("MS\d|E\d")
fields = filter(r.match, field_names)
print fields
#print field_names[3]
#print table.next()[3]
info_general = ['clave_plantel', 'clavecct', 'plantel', 'nivel', 'subnivel', 'nomcentro', 'nomdirector', 'appdirector', 'apmdirector', 'cvedepnorm', 'cvesos', 'sostenimiento', 'cveser', 'servicio', 'cvedepnorm', 'dependencia_normativa', 'modalidad', 'opcion_educativa', 'duracion', 'status_bachillerato', 'captura']
turno = ['turno','n_turno']
grupo = ['grupo1', 'grupo2', 'grupo3', 'grupo4', 'grupo5']
ubicacion = ['entidad', 'nomentidad', 'municipio', 'nommunicipio', 'localidad', 'nomlocalidad']
control=['control', 'subcontrol']
relacion_911=['OBSERVACIONES', 'RESPONSABLE_LLENADO', 'FECHA_LLENADO']
renombres = {u'clavecct':'clave',u'clave_plantel':'clave_plantel', u'nomcentro':'nombre', u'nomdirector':'nombre_director', u'nivel':'nivel', u'subnivel':'subnivel', 
u'cvedepnorm':'clave_dependencia_normativa', u'appdirector':'apellido_paterno_director',u'apmdirector':'apellido_materno_director',
u'dependencia_normativa':'dependencia_normativa', u'sostenimiento':'sostenimiento', u'cvesos':'clave_sostenimiento',  u'servicio':'servicio', 
u'municipio':'municipio', u'turno':'turno', u'n_turno':'nombre_turno', u'nomentidad':'nombre_entidad',u'C_NOM_VIALIDAD':'vialidad',u'N_EXTNUM':'num_exterior',
u'nommunicipio':'nombre_municipio', u'localidad':'localidad', u'nomlocalidad':'nombre_localidad',u'entidad':'entidad',
u'control':'control', u'subcontrol':'subcontrol', u'duracion':'duracion', u'cveser':'clave_servicio',u'status_bachillerato':'estatus', 
u'captura':'captura', u'FECHA_LLENADO':'fecha', u'opcion_educativa':'opcion_educativa', u'modalidad':'modalidad', u'RENGLON':'renglon',
u'plantel':'nombre_plantel', u'MOTIVO':'motivo', u'grupo1':'1', u'grupo2':'2', u'grupo3':'3', u'grupo4':'4', u'grupo5':'5',
u'OBSERVACIONES':'observaciones', u'RESPONSABLE_LLENADO':'responsable' }
#print len(table.field_names)
total_f = info_general+turno+ubicacion+control+relacion_911+fields
#print len(total_f)
#print (set(total_f) - set(fields))
#records = table.records
#records = table.records[1:3]
#print len(table.records)
#print records.fieldnames
#exit(0)
for record in records:
	q='CREATE VERTEX Escuela CONTENT {'
	#Informacion general
	for field in info_general:
		value = record[field]
		if type(value) == unicode or type(value) == str:
			value = value.replace('"', '\\"')
		q=q+'"%s":"%s",'%(renombres[field], value)
	q=q+'ubicacion:{'
	for field in ubicacion:
		value = record[field]
                if type(value) == unicode or type(value) == str:
                        value = value.replace('"', '\\"')
		q=q+'"%s":"%s",'%(renombres[field], value)
	q=q[:-1]
	q=q+'}, '
	q=q+'turno:{'
	for field in turno:
                value = record[field]
                if type(value) == unicode or type(value) == str:
                        value = value.replace('"', '\\"')
                q=q+'"%s":"%s",'%(renombres[field], value)
        q=q[:-1]
	q=q+'},'
	q=q+'grupo:{'
        for field in turno:
                value = record[field]
                if type(value) == unicode or type(value) == str:
                        value = value.replace('"', '\\"')
                q=q+'"%s":"%s",'%(renombres[field], value)
        q=q[:-1]
        q=q+'},'
	q=q+'control:{'
        for field in control:
                value = record[field]
                if type(value) == unicode or type(value) == str:
                        value = value.replace('"', '\\"')
                q=q+'"%s":"%s",'%(renombres[field], value)
        q=q[:-1]
        q=q+'}'
	q=q+'}'
	rid_plantel = client.command(q)[0]._rid
	#print q
	q='CREATE VERTEX 911 CONTENT {'
	for field in fields:
		value = record[field]
		#print value
		#print type(value)
		if type(value) == str:
                        try:
                                value = int(value)
                                q=q+'"%s":%i,'%(field, value)
                        except:
                                value = value.replace('"', '\\"')
                                q=q+'"%s":"%s",'%(field, value)
		elif type(value) == unicode:
                        value = value.replace('"', '\\"')
                	q=q+'"%s":"%s",'%(field, value)
		elif type(value) == datetime.date:
			q=q+'"%s":"%s",'%(field, value) 
		elif type(value) == int:
			q=q+'"%s":%i,'%(field, value)
		else:
			print "Data error @:"+rid_plantel
	#for field in resultados911:
        #        value = record[field]
        #        if type(value) == unicode:
        #                value = value.replace('"', '\\"')
	#		value = value.replace('\n', '')
	#		value = value.replace('\r', '')
        #        q=q+'"%s":"%s",'%(renombres[field], value)
        q=q[:-1]
	q=q+'}'
	#print q
	rid_911 = client.command(q)[0]._rid
	if rid_911:
                q= 'CREATE EDGE Resultado FROM %s TO %s'%(rid_plantel, rid_911)
                client.command(q)

