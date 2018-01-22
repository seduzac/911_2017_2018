# -*- coding: latin-1 -*-
import re
import datetime
import config
import csv

client = config.client
nivel = ["primaria"]
subnivel = ["general"]
csvfile = open('32_EDUCACIÓN PRIMARIA_15-01-2018.txt', 'rb',encoding="latin-1")
table = csv.reader(csvfile, quotechar ='|')
print table.next()
exit()
#print table.records[0]
r = re.compile("V\d")
fields = filter(r.match, table.field_names)
#print fields
info_general = ['CLAVECCT', 'N_CLAVECCT', 'DIRSERVREG', 'UNIDADRESP', 'PROGRAMA', 'SUBPROG', 'RENGLON', 'N_RENGLON', 'PERIODO', 'MOTIVO', 'DISPON']
turno = ['TURNO']
ubicacion = ['N_ENTIDAD', 'MUNICIPIO', 'N_MUNICIPI', 'LOCALIDAD', 'N_LOCALIDA', 'DOMICILIO', 'ZONAESCOLA', 'SECTOR']
sostenimiento = ['SOSTENIMIE']
dependencia_normativa = ['DEPNORMTVA', 'DEPADMVA']
servicio=['SERVICIO']
renombres = {u'CLAVECCT':'clave', u'N_CLAVECCT':'nombre', u'TURNO':'turno', u'N_ENTIDAD':'nombre_entidad', u'MUNICIPIO':'municipio', 
u'N_MUNICIPI':'nombre_municipio', u'LOCALIDAD':'localidad', u'N_LOCALIDA':'nombre_localidad', u'DOMICILIO':'domicilio', 
u'DEPADMVA':'depadva', u'DEPNORMTVA':'dependencia_normativa', u'ZONAESCOLA':'zona_escolar', u'SECTOR':'sector', u'DIRSERVREG':'dirservreg', 
u'SOSTENIMIE':'sostenimiento', u'SERVICIO':'servicio', u'UNIDADRESP':'unidadresp', u'PROGRAMA':'programa', u'SUBPROG':'subprog', u'RENGLON':'renglon',
 u'N_RENGLON':'nombre_renglon', u'PERIODO':'periodo', u'MOTIVO':'motivo', u'DISPON':'dispon'}
#print len(table.field_names)
total_f = info_general+turno+ubicacion+sostenimiento+dependencia_normativa+servicio+fields
#print len(total_f)
print (set(total_f) - set(table.field_names))
records = table.records
#records = table.records[1:3]
print len(table.records)
#exit(0)
for record in records:
	q='CREATE VERTEX Plantel CONTENT {'
	q=q+'"nivel":'+str(nivel)+','
	q=q+'"subnivel":'+str(subnivel)+','
	#Informacion general
	for field in info_general:
		value = record[field]
		if type(value) == unicode:
			value = value.replace('"', '\\"')
		q=q+'"%s":"%s",'%(renombres[field], value)
	q=q+'ubicacion:{'
	for field in ubicacion:
		value = record[field]
                if type(value) == unicode:
                        value = value.replace('"', '\\"')
		q=q+'"%s":"%s",'%(renombres[field], value)
	q=q[:-1]
	q=q+'}, '
	q=q+'turno:{'
	for field in turno:
                value = record[field]
                if type(value) == unicode:
                        value = value.replace('"', '\\"')
                q=q+'"%s":"%s",'%(renombres[field], value)
        q=q[:-1]
	q=q+'},'
	q=q+'sostenimiento:{'
        for field in sostenimiento:
                value = record[field]
                if type(value) == unicode:
                        value = value.replace('"', '\\"')
                q=q+'"%s":"%s",'%(renombres[field], value)
        q=q[:-1]
        q=q+'},'
	q=q+'dependencia_normativa:{'
        for field in dependencia_normativa:
                value = record[field]
                if type(value) == unicode:
                        value = value.replace('"', '\\"')
                q=q+'"%s":"%s",'%(renombres[field], value)
        q=q[:-1]
        q=q+'},'
	q=q+'servicio:{'
        for field in servicio:
                value = record[field]
                if type(value) == unicode:
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
                if type(value) == unicode:
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

