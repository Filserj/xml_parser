import pandas as pd
from datetime import datetime as dt
regions = {
##     '01':'Республика Адыгея (Адыгея)',
##     '02':'Республика Башкортостан',
##     '03':'Республика Бурятия',
##     '04':'Республика Алтай',
##     '05':'Республика Дагестан',
##     '06':'Республика Ингушетия',
##     '07':'Кабардино-Балкарская Республика',
##     '08':'Республика Калмыкия',
##     '09':'Карачаево-Черкесская Республика',
##     '10':'Республика Карелия',
##     '11':'Республика Коми',
##     '12':'Республика Марий Эл',
##     '13':'Республика Мордовия',
##     '14':'Республика Саха (Якутия)',
##     '15':'Республика Северная Осетия - Алания',
##     '16':'Республика Татарстан (Татарстан)',
##     '17':'Республика Тыва',
##     '18':'Удмуртская Республика',
##     '19':'Республика Хакасия',
##     '20':'Чеченская Республика',
##     '21':'Чувашская Республика - Чувашия',
##     '22':'Алтайский край',
##     '23':'Краснодарский край',
##     '24':'Красноярский край',
##     '25':'Приморский край',
##     '26':'Ставропольский край',
##     '27':'Хабаровский край',
##     '28':'Амурская область',
##     '29':'Архангельская область',
##     '30':'Астраханская область',
##     '31':'Белгородская область',
##     '32':'Брянская область',
##     '33':'Владимирская область',
##     '34':'Волгоградская область',
##     '35':'Вологодская область',
##     '36':'Воронежская область',
##     '37':'Ивановская область',
##     '38':'Иркутская область',
##     '39':'Калининградская область',
##     '40':'Калужская область',
##     '41':'Камчатский край',
##     '42':'Кемеровская область',
##     '43':'Кировская область',
##     '44':'Костромская область',
##     '45':'Курганская область',
##     '46':'Курская область',
##     '47':'Ленинградская область',
##     '48':'Липецкая область',
##     '49':'Магаданская область',
##     '50':'Московская область',
##     '51':'Мурманская область',
     '52':'Нижегородская область',
     '53':'Новгородская область',
     '54':'Новосибирская область',
     '55':'Омская область',
     '56':'Оренбургская область',
     '57':'Орловская область',
     '58':'Пензенская область',
     '59':'Пермский край',
     '60':'Псковская область',
     '61':'Ростовская область',
     '62':'Рязанская область',
     '63':'Самарская область',
     '64':'Саратовская область',
     '65':'Сахалинская область',
     '66':'Свердловская область',
     '67':'Смоленская область',
     '68':'Тамбовская область',
     '69':'Тверская область',
     '70':'Томская область',
     '71':'Тульская область',
     '72':'Тюменская область',
     '73':'Ульяновская область',
     '74':'Челябинская область',
     '75':'Забайкальский край',
     '76':'Ярославская область',
     '77':'Г. Москва',
     '78':'Г. Санкт-Петербург',
     '79':'Еврейская автономная область',
     '83':'Ненецкий автономный округ',
     '86':'Ханты-Мансийский автономный округ - Югра',
     '87':'Чукотский автономный округ',
     '89':'Ямало-Ненецкий автономный округ',
     '91':'Республика Крым',
     '92':'Г. Севастополь',
     '99':'Иные территории, включая город и космодром Байконур'
}
import os
# for k,v in regions.items():
#     path = 'c:/fias_xls/'+v
#     print(path)
#     os.makedirs(path)
from sqlalchemy import create_engine
import psycopg2
alchemyEngine = create_engine('postgresql+psycopg2://fias_user:fias_user@localhost:5432/fias')
dbConnection = alchemyEngine.connect()
# conn_str = """user='fias_user' dbname = 'fias' password='fias_user' host='localhost' port='5432'"""
# connection = psycopg2.connect(conn_str)
towns_sql = """
select distinct
ADDRC."SHORTNAME" as "Тип", addrc."FORMALNAME" AS "Наименование"
from FIAS.AS_aDdROBJ ADDRC
where 
ADDRC."REGIONCODE"=%(region)s 
AND ADDRC."SHORTNAME" in ( 'г', 'г.', 'рп', 'рп.', 'пгт', 'пгт.')
order by "Наименование"
"""
data_sql = """
WITH
towns as (
	select distinct 
	"AOGUID", addrc."SHORTNAME", addrc."FORMALNAME"
	from FIAS.AS_aDdROBJ ADDRC
	where ADDRC."REGIONCODE"=%(region)s AND ADDRC."SHORTNAME" in ( 'г', 'г.', 'рп', 'рп.', 'пгт', 'пгт.')
),
streets as (
	select distinct
	addr."SHORTNAME", addr."FORMALNAME",
	"PARENTGUID", "AOGUID"
	from FIAS.AS_ADDROBJ addr
	where
	addr."AOLEVEL" = '7' AND addr."REGIONCODE"=%(region)s AND ADDR."LIVESTATUS"='1'
),
street_t as (
	SELECT "SCNAME", "SOCRNAME" FROM fias.as_socrbase
	where "LEVEL"='7'
),
houses as(
	select 
		"AOGUID",
		H."HOUSENUM",
		h."BUILDNUM",
		h."STRUCNUM",
		h."HOUSEGUID",
		case when h."BUILDNUM" is null then '' else ' к'||h."BUILDNUM" end as build,
		case when h."STRUCNUM" is null then '' else ' стр'||h."STRUCNUM" end as strnum
	from fias.as_house h where H."REGIONCODE"=%(region)s and h."ESTSTATUS"='2'
),
flats AS (
	SELECT DISTINCT "HOUSEGUID" FROM FIAS.AS_ROOM WHERE "REGIONCODE"=%(region)s
)
select distinct
addrc."SHORTNAME" as "Тип", addrc."FORMALNAME" AS "Город",
case 
	when addr."SHORTNAME" in ('к-цо','кольцо','км','линия','пер.','переезд','ряд','с-к','спуск','сл','ст','ш.','ш') 
		then addr."FORMALNAME"||' '||st."SOCRNAME"
	when addr."SHORTNAME" in ('ул.','ул') 
		then addr."FORMALNAME" 
	when addr."SHORTNAME" in ('ал.','аллея','б-р','кв-л','парк','пер.','пер','пл','пр-д','наб.','наб','проезд','пр-кт','с-р','сквер') 
		then st."SOCRNAME"||' '||addr."FORMALNAME"
	else addr."FORMALNAME" 
	end AS "Улица",
H."HOUSENUM"||h.build||h.strnum as "Дом"
FROM streets addr
left join houses h on h."AOGUID" = ADDR."AOGUID" 
JOIN towns ADDRC ON ADDRC."AOGUID" = ADDR."PARENTGUID" 
left join street_t st on st."SCNAME"=addr."SHORTNAME"
join flats f on f."HOUSEGUID" = h."HOUSEGUID"
order by 1,2,3
"""
try:
    for code, name in regions.items():
    #     cursor = connection.cursor()
    #     cursor.execute(towns_sql, {'region':code})
    #     towns = cursor.fetchall()
    #     town_list = [i[1] for i in towns]
    #     print(town_list)
        file = 'c:/fias_xls/'+name+'.xlsx'
        writer = pd.ExcelWriter(file, engine='xlsxwriter')
        print('['+str(dt.now())+'] Обрабатываем регион '+name)
        df=pd.read_sql(sql=data_sql, con=dbConnection, params={'region':code})
##        df.to_excel(writer, name, index=False, columns=['Город', 'Улица', 'Дом'])
        print('['+str(dt.now())+'] Регион ' + name + ', записей ' + str(len(df)))
        town_list = df['Город'].unique()
##        print(town_list)
        for town in town_list:
            if town:
                mask = df['Город'] == town
        #         filename = 'c:/fias_xls/'+name+'.xlsx'
        #         writer = pd.ExcelWriter(filename, engine='xlsxwriter')
                df_name = town+'_df'
                df_name = df[mask]
                sheet_name = str(str(df_name['Тип'].unique()[0]) + ' ' + town.title())[:31]
                df_name.to_excel(writer, sheet_name, index=False, columns=['Город', 'Улица', 'Дом'])
                print('['+str(dt.now())+'] обработан ' + sheet_name + ', записей ' + str(len(df_name)))
            else:
                pass            
        writer.save()
        writer.close()
        print('['+str(dt.now())+'] обработан регион ' + name)
        print('=====================================================================')
except Exception as e:
    print(e)
    writer.close()
    dbConnection.close()
else:
    writer.close()
    dbConnection.close()
