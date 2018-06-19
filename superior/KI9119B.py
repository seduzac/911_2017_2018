from dbfread import DBF
import re
import config

client = config.client

query = client.query("SELECT FROM Carrera")

#for result in query:
#	print result
 
table = DBF('KI9119B.dbf', load=True, encoding="latin-1")

table2 = DBF('KI9119A.dbf', load=True, encoding="latin-1")
#print list(set(table.field_names) - set(table2.field_names))
#print table.field_names
#print table2.field_names
r = re.compile("S\d")
fields = filter(r.match, table.field_names)
#print fields
#DELETING ALL RECORDS
#client.command("TRUNCATE CLASS Institucion UNSAFE")
#client.command("TRUNCATE CLASS Empleado UNSAFE")
#client.command("TRUNCATE CLASS Responsable UNSAFE")
#client.command("TRUNCATE CLASS Direccion UNSAFE")
#client.command("TRUNCATE CLASS Resultados911 UNSAFE")
#DELETING ALL RECORDS
#Check fields
info_general = ['CARRERA', 'NOMBRECAR', 'NOMCARSINA', 'MODALIDAD', 'NOMMOD', 'PLAN_EST', 'DURACION', 'ESTRUCT', 'ESTATUS', 'MAT_2015', 'TIPO_SUB', 'SUBSISTEMA', 'Normal', 'NIVEL']
plantel = ['CLAVECCT', 'NOMBREESC']
institucion = ['CLAVEINSTI', 'NOMINSTI']
ubicacion = ['DOMICILIO', 'ENTRE_CAL', 'Y_CALLE', 'COLONIA', 'CODPOST', 'MUNICIPIO', 'NOMBREMUN', 'LOCALIDAD', 'NOMBRELOC']
contacto =['LADA', 'TELEFONO', 'TEL_EXT', 'CORREO', 'PAGINA_WEB']
sostenimiento = ['SOSTENIMIE', 'NOMBRESOS']
dependencia_normativa = ['DEPNORMTVA', 'NOMBREDEP']
servicio=['SERVICIO', 'NOMBRESER']
responsable_llenado=['resp_cap']
responsable_prog=['resp_prog']
control=['CONTROL']
resultados911=['observ', 'fecha', 'CAPTURA']
renombres = {'CARRERA':'clave', 'NOMBRECAR':'nombre', 'CLAVEINSTI':'clave_institucion','NOMINSTI':'nombre_institucion', 'CLAVECCT':'clave_plantel',
 'NOMBREESC':'nombre_plantel', 'ESTATUS':'estatus', 'MAT_2015':'MAT_2015', 'DOCEN_2015':'DOCEN_2015',
'DOMICILIO':'domicilio', 'ENTRE_CAL':'entre_calle', 'Y_CALLE':'y_calle', 'COLONIA':'colonia', 'CODPOST':'CP', 'MUNICIPIO':'municipio', 
'NOMBREMUN':'nombre_municipio', 'LOCALIDAD':'localidad', 'NOMBRELOC':'nombre_localidad', 'LADA':'lada', 'TELEFONO':'telefono',
 'TEL_EXT':'telefono_extension', 'FAX':'fax', 'FAX_EXT':'fax_extension', 'CORREO':'correo', 'PAGINA_WEB':'pagina_web',
'SOSTENIMIE':'sostenimiento', 'NOMBRESOS':'nombre', 'DEPNORMTVA':'dependencia_normativa', 'NOMBREDEP':'nombre', 'SERVICIO':'servicio',
 'NOMBRESER':'nombre', 'NIVEL':'nivel', 'resp_cap':'responsable_captura', 'resp_prog':'responsable_programa', 'CONTROL':'control',
'observ':'observaciones', 'fecha':'fecha', 'CAPTURA':'captura', 'Normal':'normal', 'NOMCARSINA':'nomcarsina', 'MODALIDAD':'modalidad',
 'NOMMOD':'nombre_modalidad', 'PLAN_EST':'plan_estudios', 'DURACION':'duracion', 'ESTRUCT':'estructura', 'TIPO_SUB':'tipo_subsistema', 'SUBSISTEMA':'subsistema'}
print len(table.field_names)
print len(plantel+institucion+info_general+ubicacion+contacto+sostenimiento+dependencia_normativa+servicio+responsable_llenado+responsable_prog+control+resultados911)
print len(fields)
for record in table.records:
	if record['CLAVECCT'] != "":
                q='SELECT FROM Plantel WHERE clave = "%s"'%record['CLAVECCT']
                plantel_bd = client.query(q,1)
                if len(plantel_bd) == 0:
                        print "Carrera sin plantel"
                else:
                        rid_plantel= plantel_bd[0]._rid
	if record['CLAVEINSTI'] != "":
                q='SELECT FROM Institucion WHERE clave = "%s"'%record['CLAVEINSTI']
                institucion_bd = client.query(q,1)
                if len(institucion_bd) == 0:
                        print "Plantel sin Institucion"
                else:
                        rid_institucion= institucion_bd[0]._rid
	if record['resp_prog'] != "":
		q='SELECT FROM Empleado WHERE nombre = "%s"'%record['resp_prog']
		responsable_bd = client.query(q,1)
		if len(responsable_bd) == 0:
			q = 'CREATE VERTEX Empleado CONTENT {"nombre":"%s"}'%record['resp_prog']
			rid_responsable_prog= client.command(q)[0]._rid
		else:
			rid_responsable_prog= responsable_bd[0]._rid
	if record['resp_cap'] != "":
                q='SELECT FROM Empleado WHERE nombre = "%s"'%record['resp_cap']
                responsable_cap_bd = client.query(q,1)
                if len(responsable_cap_bd) == 0:
                        q = 'CREATE VERTEX Empleado CONTENT {"nombre":"%s"}'%record['resp_cap']
                        rid_responsable_cap= client.command(q)[0]._rid
                else:
                        rid_responsable_cap= responsable_cap_bd[0]._rid
	q='CREATE VERTEX Carrera CONTENT {'
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
	q=q+'contacto:{'
	for field in contacto:
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
        q=q+'},'
	q=q+'control:{'
        for field in control:
                value = record[field]
                if type(value) == unicode:
                        value = value.replace('"', '\\"')
                q=q+'"%s":"%s",'%(renombres[field], value)
        q=q[:-1]
        q=q+'}'
	q=q+'}'
	rid_carrera = client.command(q)[0]._rid
	q='CREATE VERTEX Resultados911 CONTENT {'
	for field in fields:
		value = record[field]
                if type(value) == unicode:
                        value = value.replace('"', '\\"')
                	q=q+'"%s":"%s",'%(field, value)
		else:
			q=q+'"%s":%i,'%(field, value)
	for field in resultados911:
                value = record[field]
                if type(value) == unicode:
                        value = value.replace('"', '\\"')
			value = value.replace('\n', '')
			value = value.replace('\r', '')
                q=q+'"%s":"%s",'%(renombres[field], value)
        q=q[:-1]
	q=q+'}'
	#print q
	rid_911 = client.command(q)[0]._rid
	if rid_responsable_prog:
		q= 'CREATE EDGE Responsable FROM %s TO %s'%(rid_responsable_prog, rid_carrera)
		client.command(q)
	if rid_responsable_cap:
                q= 'CREATE EDGE Responsable FROM %s TO %s'%(rid_responsable_cap, rid_911)
                client.command(q)
	if rid_911:
                q= 'CREATE EDGE Resultado FROM %s TO %s'%(rid_carrera, rid_911)
                client.command(q)
	if rid_plantel:
                q= 'CREATE EDGE Ofrece FROM %s TO %s'%(rid_plantel, rid_carrera)
                client.command(q)

