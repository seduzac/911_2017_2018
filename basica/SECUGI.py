# -*- coding: latin-1 -*-
import re
import datetime
import config
import csv

client = config.client
nivel = ["secundaria"]
subnivel = ["general"]
csvfile = open('32_EDUCACIÓN SECUNDARIA_19-03-2018.txt', 'rb')
table = csv.reader(csvfile, delimiter ='|')
field_names = table.next()
records = csv.DictReader(csvfile, fieldnames=field_names, delimiter ='|')
#print field_names
#print table.records[0]
r = re.compile("V\d|E\d")
fields = filter(r.match, field_names)
#print fields
#print field_names[3]
#print table.next()[3]
info_general = ['CV_CCT', 'NOMBRECT', 'TIPO', 'NIVEL', 'SUBNIVEL', 'CV_CARACTERIZAN1', 'C_CARACTERIZAN1', 'CV_CARACTERIZAN2','C_CARACTERIZAN2', 'PERIODO', 'ZONA', 'JEFSEC', 'SERVREG']
turno = ['TURNO','CV_TURNO']
ubicacion = ['CV_MUN', 'C_NOM_MUN', 'CV_LOC', 'C_NOM_LOC', 'C_NOM_VIALIDAD', 'N_EXTNUM']
control=['CONTROL', 'SUBCONTROL']
relacion_911=['CV_ESTATUS_CAPTURA', 'FECHA_ENTREGA']
renombres = {u'CV_CCT':'clave', u'NOMBRECT':'nombre', u'TIPO':'tipo', u'NIVEL':'nivel', u'SUBNIVEL':'subnivel', u'CV_CARACTERIZAN1':'cv_caracterizan1',
u'C_CARACTERIZAN1':'c_caracterizan1', u'CV_CARACTERIZAN2':'cv_caracterizan2', u'C_CARACTERIZAN2':'c_caracterizan2',  u'JEFSEC':'jefsec', 
u'CV_MUN':'municipio', u'TURNO':'turno', u'CV_TURNO':'cv_turno', u'C_NOM_ENT':'entidad',u'C_NOM_VIALIDAD':'vialidad',u'N_EXTNUM':'num_exterior',
u'C_NOM_MUN':'nombre_municipio', u'CV_LOC':'localidad', u'C_NOM_LOC':'nombre_localidad',
u'CONTROL':'control', u'SUBCONTROL':'subcontrol', u'ZONA':'zona', u'SERVREG':'servreg', 
u'CV_ESTATUS_CAPTURA':'estatus_captura', u'FECHA_ENTREGA':'fecha', u'PROGRAMA':'programa', u'SUBPROG':'subprog', u'RENGLON':'renglon',
u'PERIODO':'periodo', u'MOTIVO':'motivo'}
#print len(table.field_names)
total_f = info_general+turno+ubicacion+control+relacion_911+fields
print len(total_f)
print (set(total_f) - set(fields))
#records = table.records
#records = table.records[1:3]
#print len(table.records)
#print records.fieldnames
for record in records:
	q='CREATE VERTEX Plantel CONTENT {'
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
	q='CREATE VERTEX Resultados911 CONTENT {'
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
                q= 'CREATE EDGE Resultado FROM %s TO %s SET '%(rid_plantel, rid_911)
                for field in relacion_911:
                        value = record[field]
                        value = value.replace('"', '\\"')
                        q=q+'%s = "%s",'%(field, value)
                q=q[:-1]
                client.command(q)
