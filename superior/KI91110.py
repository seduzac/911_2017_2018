from dbfread import DBF
import re
import config

client = config.client
 
table = DBF('KI91110.dbf', load=True, encoding="latin-1")
#print table.field_names
r = re.compile("S\d")
fields = filter(r.match, table.field_names)
#print fields
nivel = "superior"
info_general = ['CLAVEINSTI', 'NOMBREINS', 'ESTATUS', 'MAT_2015', 'DOCEN_2015']
ubicacion = ['DOMICILIO', 'ENTRE_CAL', 'Y_CALLE', 'COLONIA', 'CODPOST', 'MUNICIPIO', 'NOMBREMUN', 'LOCALIDAD', 'NOMBRELOC']
contacto =['LADA', 'TELEFONO', 'TEL_EXT', 'FAX', 'FAX_EXT', 'CORREO', 'PAGINA_WEB']
sostenimiento = ['SOSTENIMIE', 'NOMBRESOS']
dependencia_normativa = ['DEPNORMTVA', 'NOMBREDEP']
servicio=['SERVICIO', 'NOMBRESER']
director=['NOMDIREC']
responsable_llenado=['resp_cap']
responsable_prog=['resp_prog']
control=['CONTROL']
resultados911=['observ', 'fecha', 'CAPTURA']
renombres = {'CLAVEINSTI':'clave','NOMBREINS':'nombre', 'ESTATUS':'estatus', 'MAT_2015':'MAT_2015', 'DOCEN_2015':'DOCEN_2015',
'DOMICILIO':'domicilio', 'ENTRE_CAL':'entre_calle', 'Y_CALLE':'y_calle', 'COLONIA':'colonia', 'CODPOST':'CP', 'MUNICIPIO':'municipio', 
'NOMBREMUN':'nombre_municipio', 'LOCALIDAD':'localidad', 'NOMBRELOC':'nombre_localidad', 'LADA':'lada', 'TELEFONO':'telefono',
 'TEL_EXT':'telefono_extension', 'FAX':'fax', 'FAX_EXT':'fax_extension', 'CORREO':'correo', 'PAGINA_WEB':'pagina_web',
'SOSTENIMIE':'sostenimiento', 'NOMBRESOS':'nombre', 'DEPNORMTVA':'dependencia_normativa', 'NOMBREDEP':'nombre', 'SERVICIO':'servicio',
 'NOMBRESER':'nombre', 'NOMDIREC':'direccion', 'resp_cap':'responsable_captura', 'resp_prog':'responsable_programa', 'CONTROL':'control',
'observ':'observaciones', 'fecha':'fecha', 'CAPTURA':'captura'}
print len(table.field_names)
print len(info_general+ubicacion+contacto+sostenimiento+dependencia_normativa+servicio+director+responsable_llenado+responsable_prog+control+resultados911)
print len(fields)
for record in table.records:
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
	if record['NOMDIREC'] != "":
                q='SELECT FROM Empleado WHERE nombre = "%s"'%record['NOMDIREC']
                responsable_bd = client.query(q,1)
                if len(responsable_bd) == 0:
                        q = 'CREATE VERTEX Empleado CONTENT {"nombre":"%s"}'%record['NOMDIREC']
                        rid_direccion= client.command(q)[0]._rid
                else:
                        rid_direccion= responsable_bd[0]._rid
	q='CREATE VERTEX Institucion CONTENT {'
	q=q+'"nivel":"'+nivel+'",'
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
	rid_institucion = client.command(q)[0]._rid
	q='CREATE VERTEX Resultados911 CONTENT {'
	for field in fields:
		value = record[field]
                if type(value) == unicode:
                        value = value.replace('"', '\\"')
                	q=q+'"%s":"%s",'%(field, value)
			print value
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
		q= 'CREATE EDGE Responsable FROM %s TO %s'%(rid_responsable_prog, rid_institucion)
		client.command(q)
	if rid_direccion:
                q= 'CREATE EDGE Direccion FROM %s TO %s'%(rid_direccion, rid_institucion)
                client.command(q)
	if rid_911:
                q= 'CREATE EDGE Resultado FROM %s TO %s'%(rid_institucion, rid_911)
                client.command(q)
	if rid_responsable_cap:
                q= 'CREATE EDGE Responsable FROM %s TO %s'%(rid_responsable_cap, rid_911)
                client.command(q)
