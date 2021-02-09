# datojen haku yhes koos
# työttömyysturva maakunnittain
# maakunnat maksukuukausilta, että tuensaaja tilastoituu järkevästi siihen maakuntaan jossa on tukea saanut ja jos on muuttanut, niin tilastoituu useampaan

if (!file.exists("./datalst.RDS")){
  
sql_query <- "with perusjoukko as
	(select
	distinct ASIAKAS_ID
	,mapv
	,maksukuukausi
	,ratkaisu_jno
	,maakunta_nimi

	from DMPRODETUUSDATA.KADMIN.TIL_SAAJAT_RF

	where aikatyyppi = 'KK'
	and maksettu_eur > 0)

, tarkistus as
	(select distinct asiakas_id
	,mapv
	from DMPRODETUUSDATA.KADMIN.TIL_SAAJAT_RF
	where aikatyyppi = 'KK'
	and maksettu_eur > 0

	union all 
	select distinct id asiakas_id
	,mapv
	from TILASTOINTITIEDOSTOT.TYOTTOMYYSTURVA.DF_FMA_VV2018
	where YHTE > 0)

,koonti as
	(select
	perusjoukko.asiakas_id
	,perusjoukko.mapv
	,perusjoukko.maksukuukausi
	,perusjoukko.ratkaisu_jno
	,perusjoukko.maakunta_nimi
	,tarkistus.asiakas_id tarkistus_id

	from perusjoukko

	left join tarkistus
	on tarkistus.asiakas_id = perusjoukko.asiakas_id
	and tarkistus.mapv <= last_day(add_months(perusjoukko.mapv,-1))
	and tarkistus.mapv > last_day(add_months(perusjoukko.mapv,-13))
	
	where maksukuukausi >= 201901)

,yrittajat as
	(select
	saajat.ASIAKAS_ID
	,saajat.RATKAISU_JNO
	,saajat.MAPV
	,saajat.MAKSUKUUKAUSI
		
	from
		--YK-lausunnon saaneet
		(select ASIAKAS_ID, JNO,
		row_number() over(partition by ASIAKAS_ID, JNO order by LATAUSNRO desc, LATAUSPV desc, RLAIKA desc) rivi
		from STAGEPROD.KADMIN.STG_TILAUSI
		where laukd = '0YK') tilausi
		
		--Lausunnon yhdistäminen maksuriveihin, valmisteluun otetut lausunnot.
		join
		(SELECT ASIAKAS_ID, JNO, TRATJNO,
		row_number() over (partition by ASIAKAS_ID, EJ, AJNO, LAJNO order by LATAUSNRO desc, LATAUSPV desc, RLAIKA desc) as rivi
		FROM STAGEPROD.KADMIN.STG_RFLAUSU
		where PRATJNO is NULL and KUITTI = 'V') as rflausu
		using(asiakas_id,JNO)
		
		--Työmarkkinatukea 2020 lähtien saaneet.
		join
		(select ASIAKAS_ID, MAPV, MAKSUKUUKAUSI, RATKAISU_JNO, ETUUS_TILASTO
		from DMPRODETUUSDATA.KADMIN.TIL_SAAJAT_RF
		where MAKSETTU_EUR > 0
		and ETUUS_TILASTO = 'T'
		and extract(year from mapv) >= 2020) saajat
		on saajat.ASIAKAS_ID = rflausu.ASIAKAS_ID and saajat.RATKAISU_JNO = rflausu.TRATJNO
		
	where 
		(rflausu.rivi = 1 or rflausu.rivi is null)
		and (tilausi.rivi = 1 or tilausi.rivi is null))

select
coalesce(maakunta_nimi,'Koko maa') as maakunta
, null as sukupuoli
, null as ikaryhma
,vuosi 
,kaikki 
,yrittajat 
,uudet_ei_yrittajat as uudet
,vanhat_ei_yrittajat  as aiemmin_saaneet

from
	(select
	maakunta_nimi
	,substr(koonti.maksukuukausi, 1,4) as vuosi
	,count(distinct koonti.asiakas_id) kaikki
	,count(distinct case when yrittajat.asiakas_id is not null then koonti.asiakas_id end) yrittajat
	,count(distinct case when yrittajat.asiakas_id is null and tarkistus_id is null then koonti.asiakas_id end) uudet_ei_yrittajat
	,kaikki-(yrittajat+uudet_ei_yrittajat) vanhat_ei_yrittajat


	from koonti

	left join yrittajat
	using(asiakas_id,ratkaisu_jno)
	where koonti.maksukuukausi between 201901 and 202012-- tätä muuttamalla saa eri vuodet!
	group by cube(maakunta_nimi),vuosi
	having maakunta_nimi is null or maakunta_nimi not in ('Ahvenanmaa','Ulkom. ja tuntematon')
	order by maakunta_nimi, vuosi
	) alikysely
	
order by 
	case when maakunta_nimi is null then 'AAA'
	else maakunta_nimi end
	,vuosi
;"

data1 <- get_query(sql_query) %>% 
  janitor::clean_names()

# työttömyysturva iän ja sukupuolen mukaan
# nämä tiedot taas haetaan vuoden  lopulta, koska ei ole järkeä tilastoitua useampaan ryhmään (erityisesti ikä)

sql_query <- "with asiakas as
	(SELECT 
	asiakas_id
	,(case when sukupuoli_koodi='1' then 'Miehet'
	when sukupuoli_koodi='2' then 'Naiset' else 'muu' end) as sukupuoli
	, date_part('years',age('2019-12-31',syntyma_pvm)) as ika
	, maakunta_nimi
	, 2019 as vuosi
	FROM DMPROD00.KADMIN.V_D_ASIAKAS as hlo
	left join  DMPROD00.KADMIN.D_ALUE as alue
	on hlo.vakituisen_asuinkunnan_nro=alue.korvaava_kunnan_nro
	WHERE 1=1 
	AND '2019-12-31' BETWEEN hlo.ALKU_PVM AND hlo.LOPPU_PVM 
	and asiakas_id <>-1
	and POIKKILEIKKAUS_VUOSI=2019
	union
	SELECT 
	asiakas_id
	,(case when sukupuoli_koodi='1' then 'Miehet'
	when sukupuoli_koodi='2' then 'Naiset' else 'muu' end) as sukupuoli
	, date_part('years',age('2020-12-31',syntyma_pvm)) as ika
	, maakunta_nimi
	, 2020 as vuosi
	FROM DMPROD00.KADMIN.V_D_ASIAKAS as hlo
	left join  DMPROD00.KADMIN.D_ALUE as alue
	on hlo.vakituisen_asuinkunnan_nro=alue.korvaava_kunnan_nro
	WHERE 1=1 
	AND '2020-12-31' BETWEEN hlo.ALKU_PVM AND hlo.LOPPU_PVM 
	and asiakas_id <>-1
	and POIKKILEIKKAUS_VUOSI=2020
	) 
	
, perusjoukko as
	(select
	distinct ASIAKAS_ID
	,mapv
	,maksukuukausi
	,substr(maksukuukausi, 1, 4) as vuosi
	,ratkaisu_jno
	
	from DMPRODETUUSDATA.KADMIN.TIL_SAAJAT_RF

	where aikatyyppi = 'KK'
	and maksettu_eur > 0)

, tarkistus as
	(select distinct asiakas_id
	,mapv
	from DMPRODETUUSDATA.KADMIN.TIL_SAAJAT_RF
	where aikatyyppi = 'KK'
	and maksettu_eur > 0

	union all 
	select distinct id asiakas_id
	,mapv
	from TILASTOINTITIEDOSTOT.TYOTTOMYYSTURVA.DF_FMA_VV2018
	where YHTE > 0)

,koonti as
	(select distinct
	perusjoukko.asiakas_id
	,perusjoukko.mapv
	,perusjoukko.maksukuukausi
	,perusjoukko.ratkaisu_jno
	,tarkistus.asiakas_id tarkistus_id
	,tarkistus.mapv tarkistus_mapv
	,asiakas.sukupuoli
	,(case 
		when asiakas.ika < 25 then 'alle 25'
		when asiakas.ika between 25 and 34 then '25-34'
		when asiakas.ika between 35 and 44 then '35-44'
		when asiakas.ika between 45 and 54 then '45-54'
		when asiakas.ika > 54 then 'yli 54'
	end) as ikaryhma

	from perusjoukko

	left join tarkistus
	on tarkistus.asiakas_id = perusjoukko.asiakas_id
	and tarkistus.mapv <= last_day(add_months(perusjoukko.mapv,-1))
	and tarkistus.mapv > last_day(add_months(perusjoukko.mapv,-13))
	left join asiakas
	on perusjoukko.asiakas_id=asiakas.asiakas_id and perusjoukko.vuosi=asiakas.vuosi
	
	where maksukuukausi >= 201901)
	
, yrittajat as (
	select
	saajat.ASIAKAS_ID
	,saajat.RATKAISU_JNO
	,saajat.MAPV
	,saajat.MAKSUKUUKAUSI

	from
		--YK-lausunnon saaneet
		(select ASIAKAS_ID, JNO,
		row_number() over(partition by ASIAKAS_ID, JNO order by LATAUSNRO desc, LATAUSPV desc, RLAIKA desc) rivi
		from STAGEPROD.KADMIN.STG_TILAUSI
		where laukd = '0YK') tilausi
		
		--Lausunnon yhdistäminen maksuriveihin, valmisteluun otetut lausunnot.
		join
		(SELECT ASIAKAS_ID, JNO, TRATJNO,
		row_number() over (partition by ASIAKAS_ID, EJ, AJNO, LAJNO order by LATAUSNRO desc, LATAUSPV desc, RLAIKA desc) as rivi
		FROM STAGEPROD.KADMIN.STG_RFLAUSU
		where PRATJNO is NULL and KUITTI = 'V') as rflausu
		on rflausu.ASIAKAS_ID = tilausi.ASIAKAS_ID and rflausu.JNO = tilausi.JNO
		
		--Työmarkkinatukea 2020 lähtien saaneet.
		join
		(select ASIAKAS_ID, MAKSUKUUKAUSI, RATKAISU_JNO, MAPV
		from DMPRODETUUSDATA.KADMIN.TIL_SAAJAT_RF
		where MAKSETTU_EUR > 0
		and ETUUS_TILASTO = 'T'
		and extract(year from mapv) >= 2020) saajat
		on saajat.ASIAKAS_ID = rflausu.ASIAKAS_ID and saajat.RATKAISU_JNO = rflausu.TRATJNO
		
	where 
		(rflausu.rivi = 1 or rflausu.rivi is null)
		and (tilausi.rivi = 1 or tilausi.rivi is null))
		
select
null as maakunta
, coalesce(sukupuoli,'kaikki') as sukupuoli
,coalesce(ikaryhma,'kaikki') as ikaryhma
,vuosi 
,kaikki 
,yrittajat 
,uudet_ei_yrittajat as uudet
,vanhat_ei_yrittajat as aiemmin_saaneet

from
	(select
	sukupuoli
	,ikaryhma
	,substr(koonti.maksukuukausi, 1,4) as vuosi
	,count(distinct koonti.asiakas_id) kaikki
	,count(distinct case when yrittajat.asiakas_id is not null then koonti.asiakas_id end) yrittajat
	,count(distinct case when yrittajat.asiakas_id is null and tarkistus_id is null then koonti.asiakas_id end) uudet_ei_yrittajat
	,kaikki-(yrittajat+uudet_ei_yrittajat) vanhat_ei_yrittajat


	from koonti

	left join yrittajat
	using(asiakas_id,ratkaisu_jno)
	where koonti.maksukuukausi between 201901 and 202012-- tätä muuttamalla saa eri vuodet!
	group by cube(sukupuoli,ikaryhma),vuosi

	order by sukupuoli, ikaryhma, vuosi
	) alikysely
	
order by 
	case when sukupuoli = 'kaikki' then 'A'
		else sukupuoli end
		,case ikaryhma
			when 'kaikki' then '0'
			when 'alle 25' then '1'
			when 'yli 54' then '5'
		else ikaryhma end
		,vuosi
	;"


data2 <- get_query(sql_query) %>% 
  janitor::clean_names()

# yhdistetään
tyottomyysturva <- bind_rows(data1, data2)

# toimeentulotuki maakunnittain
# maakunnat maksukuukaudelta, että vuoden alun uusimaalainen totunsaaja ei tilastoidu keski-pohjanmaalle, jos on sattunut muuttamaan sinne vuoden aikana

sql_query <- "with saajat as
	(select distinct
	coalesce(KOTITAL_MAAKUNTA_NIMI,'Koko maa') maakunta
	,vuosi
	,saajat
	,uudet_saajat
	from
		(select
		KOTITAL_MAAKUNTA_NIMI
		,substr(maksukuukausi, 1, 4) as vuosi
		,count(distinct asiakas_id) saajat
		,count(distinct case when SAAJA_UUSI = 1 then asiakas_id end) uudet_saajat

		from DMPROD00.KADMIN.V_AINEISTO_F_SAAJAT_PC

		where 
		saaja_lkm = 1
		and substr(maksukuukausi,1,4) >= 2019

		group by cube(KOTITAL_MAAKUNTA_NIMI), vuosi
		having kotital_maakunta_nimi is null or kotital_maakunta_nimi not in ('Ahvenanmaa','Ulkom. ja tuntematon')) vali
	)

,kotitaloudet as
	(select distinct
	coalesce(maakunta_nimi,'Koko maa') maakunta
	,vuosi
	,kotital
	,uudet_kotital
	from
		(select 
		maakunta_nimi
		,substr(maksukuukausi, 1, 4) as vuosi
		,count(distinct KOTITALOUS_ID) kotital
		,count(distinct case when KOTITALOUS_UUSI = 1 then KOTITALOUS_ID end) uudet_kotital

		from DMPROD00.KADMIN.V_AINEISTO_F_SAAJAT_KOTITALOUS_PC

		where 
		KOTITALOUS_LKM = 1
		and substr(maksukuukausi,1,4) >= 2019

		group by cube(maakunta_nimi),vuosi
		having maakunta_nimi is null or maakunta_nimi not in ('Ahvenanmaa','Ulkom. ja tuntematon')) vali
	)

select
maakunta 
, null as sukupuoli
, null as ikaryhma
,vuosi
,saajat as kaikki
,uudet_saajat as uudet
,saajat-uudet_saajat as aiemmin_saaneet
,kotital as kaikki_kotitaloudet
,uudet_kotital as uudet_kotitaloudet
,kotital-uudet_kotital  as aiemmin_saaneet_kotitaloudet

from saajat

left join kotitaloudet
using(maakunta, vuosi)

order by
case when maakunta = 'Koko maa' then 'A'
	else maakunta end
,vuosi"

data1 <- get_query(sql_query) %>% 
  janitor::clean_names()

# toimeentulotuki iän ja sukupuolen mukaan, pelkät saajat

sql_query <- "with asiakas as
	(SELECT 
	asiakas_id
	,(case when sukupuoli_koodi='1' then 'Miehet'
	when sukupuoli_koodi='2' then 'Naiset' else 'muu' end) as sukupuoli
	, date_part('years',age('2019-12-31',syntyma_pvm)) as ika
	, maakunta_nimi
	, 2019 as vuosi
	FROM DMPROD00.KADMIN.V_D_ASIAKAS as hlo
	left join  DMPROD00.KADMIN.D_ALUE as alue
	on hlo.vakituisen_asuinkunnan_nro=alue.korvaava_kunnan_nro
	WHERE 1=1 
	AND '2019-12-31' BETWEEN hlo.ALKU_PVM AND hlo.LOPPU_PVM 
	and asiakas_id <>-1
	and POIKKILEIKKAUS_VUOSI=2019
	union
	SELECT 
	asiakas_id
	,(case when sukupuoli_koodi='1' then 'Miehet'
	when sukupuoli_koodi='2' then 'Naiset' else 'muu' end) as sukupuoli
	, date_part('years',age('2020-12-31',syntyma_pvm)) as ika
	, maakunta_nimi
	, 2020 as vuosi
	FROM DMPROD00.KADMIN.V_D_ASIAKAS as hlo
	left join  DMPROD00.KADMIN.D_ALUE as alue
	on hlo.vakituisen_asuinkunnan_nro=alue.korvaava_kunnan_nro
	WHERE 1=1 
	AND '2020-12-31' BETWEEN hlo.ALKU_PVM AND hlo.LOPPU_PVM 
	and asiakas_id <>-1
	and POIKKILEIKKAUS_VUOSI=2020
	order by 1
	) 

, saajat as
	(select 
	coalesce(sukupuoli,'kaikki')  as sukupuoli
	,coalesce(ikaryhma,'kaikki')  as ikaryhma
	, vuosi
	, count(distinct asiakas_id) as saajat
	, count(distinct case when saaja_uusi=1 then asiakas_id end) as uudet
	from 	
	
		(select distinct
		asiakas_id
		, sukupuoli
		,case   when length(ika) > 5 then 'Tieto puuttuu'
				when ika < 25 then 'alle 25'
				when ika between 25 and 34 then '25-34'
				when ika between 35 and 44 then '35-44'
				when ika between 45 and 54 then '45-54'
				when ika > 54 then 'yli 54'
			end as ikaryhma
		,saaja_uusi
		,vuosi
		from
			(select
			asiakas_id
			,saaja_uusi
			,substr(maksukuukausi, 1, 4) as vuosi
			from DMPROD00.KADMIN.V_AINEISTO_F_SAAJAT_PC
			where 
			saaja_lkm = 1
			and substr(maksukuukausi,1,4) >= 2019
			) as vali1
			left join asiakas
			using(asiakas_id, vuosi)
			) as vali2
			
	group by cube(sukupuoli,ikaryhma),vuosi
	having ikaryhma is null or ikaryhma <> 'Tieto puuttuu'
	order by 1, 2
		)


select 
null as maakunta
, sukupuoli
, ikaryhma
, vuosi
, saajat as kaikki
, uudet
, saajat-uudet as aiemmin_saaneet
from saajat
where saajat>3

order by 
	case when sukupuoli = 'kaikki' then 'a'
	else sukupuoli end
	,case ikaryhma
		when 'kaikki' then '0'
		when 'alle 25' then '1'
		when 'yli 54' then '5'
	else ikaryhma end
	,vuosi
	;"

data2 <- get_query(sql_query) %>% 
  janitor::clean_names()

#yhdistetään
toimeentulotuki <- bind_rows(data1, data2)


# asumistuki kans kaikille kuukausille maakunnittain

sql_query <- "with data as
	(select
	asiakas_id
	,maksukuukausi
	,maakunta_nro

	from DMPRODETUUSDATA.KADMIN.TIL_SAAJAT_AY

	where AIKATYYPPI = 'KK' 
	and MATY = 'S'
	and MAKSETTU_EUR > 0

	union all
	select
	id
	,v4kk
	,mkunta

	from TILASTOINTITIEDOSTOT.ASUMISTUET.YD_AMA_VV2019

	where maty = 'S'
	and astuki > 0
	and v4kk between 201901 and 201911
	)

,tarkistus as
	(select
	asiakas_id tarkistus_id
	,maksukuukausi tarkistuskuukausi

	from DMPRODETUUSDATA.KADMIN.TIL_SAAJAT_AY

	where AIKATYYPPI = 'KK' 
	and MATY = 'S'
	and MAKSETTU_EUR > 0

	union all
	select
	id asiakas_id
	,v4kk maksukuukausi

	from TILASTOINTITIEDOSTOT.ASUMISTUET.YD_AMA_VV2019

	where maty = 'S'
	and astuki > 0
	and v4kk < 201912 --TIL-taulusta 201912 ja myöhemmät
	
	union all
	select
	id
	,v4kk

	from TILASTOINTITIEDOSTOT.ASUMISTUET.YD_AMA_VV2018

	where maty = 'S'
	and astuki > 0

	order by 1,2
	)

,alue as
	(SELECT 
	distinct maakunta_nro
	,maakunta_nimi
	FROM DMPROD00.KADMIN.D_ALUE
	WHERE 
	ETL2_RIVI_VOIMASSA = 'K'
	and POIKKILEIKKAUS_VOIMASSA = 'K')

select
coalesce(maakunta,'koko maa')  as maakunta
,vuosi
,kaikki 
,uudet 
,kaikki-uudet as aiemmin_saaneet

from
	(select
	maakunta_nimi maakunta
	,substr(maksukuukausi, 1,4) as vuosi
	,count(distinct asiakas_id) kaikki
	,count(distinct case when tarkistus_id is null then asiakas_id end) uudet
	
	from data

	left join
		(select
		tarkistus_id
		,tarkistuskuukausi
		from tarkistus)
		tarkistus
	on tarkistus.tarkistus_id = data.asiakas_id
	and (substr(tarkistus.tarkistuskuukausi,1,4)||'-'||substr(tarkistus.tarkistuskuukausi,5,2)||'-01')::date
	between add_months(substr(data.maksukuukausi,1,4)||'-'||substr(data.maksukuukausi,5,2)||'-01',-12)
	and last_day(add_months(substr(data.maksukuukausi,1,4)||'-'||substr(data.maksukuukausi,5,2)||'-01',-1))

	left join alue
	using(maakunta_nro)
	
	group by cube(1),2
	having maakunta_nimi is null or maakunta_nimi not in ('Ahvenanmaa','Ulkom. ja tuntematon')
	) alikysely

order by
	case when maakunta is null then 'AAA'
	else maakunta end
	,vuosi
	;"

# vain yksi data
asumistuki <- get_query(sql_query) %>% 
  janitor::clean_names()

# sairauspäivärahat, maakunnittain maksukuukausilta

sql_query <- "select
coalesce(maakunta_nimi,'Koko maa') as maakunta
, null as sukupuoli
, null as ikaryhma
,substr(maksukuukausi, 1, 4) as vuosi
,count(distinct case when saajat.ETUUS = 'Sairauspäiväraha' then saajat.ASIAKAS_ID end)  as sairauspaivaraha
,count(distinct case when saajat.etuus = 'Tartuntatautipäiväraha' and (EPIDEMIATUKI <> 'Kyllä' or EPIDEMIATUKI is null) then saajat.ASIAKAS_ID end) as tartuntatautipaivaraha
,count(distinct case when EPIDEMIATUKI = 'Kyllä' then saajat.ASIAKAS_ID end)  as epidemiatuki

from DMPROD00.KADMIN.V_AINEISTO_F_SAAJAT_SJ saajat

left join DMPROD00.KADMIN.V_AINEISTO_F_RATKAISUT_SJ ratkaisut
using(RATKAISUT_SJ_SG)

where SAAJA_LKM = 1
and maksukuukausi >= 201901

group by cube(maakunta_nimi), vuosi
having maakunta_nimi is null or maakunta_nimi not in ('Ahvenanmaa','Ulkom. ja tuntematon')
order by 
	case when maakunta_nimi is null then 'AAA'
	else maakunta_nimi end
,vuosi
;"

data1 <- get_query(sql_query) %>% 
  janitor::clean_names()

# sairauspäivärahat iän ja sukupuolen mukaan

sql_query <- "with asiakas as
	(SELECT 
	asiakas_id
	,(case when sukupuoli_koodi='1' then 'Miehet'
	when sukupuoli_koodi='2' then 'Naiset' else 'muu' end) as sukupuoli
	, date_part('years',age('2019-12-31',syntyma_pvm)) as ika
	, maakunta_nimi
	, 2019 as vuosi
	FROM DMPROD00.KADMIN.V_D_ASIAKAS as hlo
	left join  DMPROD00.KADMIN.D_ALUE as alue
	on hlo.vakituisen_asuinkunnan_nro=alue.korvaava_kunnan_nro
	WHERE 1=1 
	AND '2019-12-31' BETWEEN hlo.ALKU_PVM AND hlo.LOPPU_PVM 
	and asiakas_id <>-1
	and POIKKILEIKKAUS_VUOSI=2019
	union
	SELECT 
	asiakas_id
	,(case when sukupuoli_koodi='1' then 'Miehet'
	when sukupuoli_koodi='2' then 'Naiset' else 'muu' end) as sukupuoli
	, date_part('years',age('2020-12-31',syntyma_pvm)) as ika
	, maakunta_nimi
	, 2020 as vuosi
	FROM DMPROD00.KADMIN.V_D_ASIAKAS as hlo
	left join  DMPROD00.KADMIN.D_ALUE as alue
	on hlo.vakituisen_asuinkunnan_nro=alue.korvaava_kunnan_nro
	WHERE 1=1 
	AND '2020-12-31' BETWEEN hlo.ALKU_PVM AND hlo.LOPPU_PVM 
	and asiakas_id <>-1
	and POIKKILEIKKAUS_VUOSI=2020
	order by 1
	) 

,saajat as 
	(select
	saajat.asiakas_id
	, substr(maksukuukausi, 1, 4) as vuosi
	, saajat.etuus
	, epidemiatuki
	from DMPROD00.KADMIN.V_AINEISTO_F_SAAJAT_SJ as saajat
	left join DMPROD00.KADMIN.V_AINEISTO_F_RATKAISUT_SJ as ratkaisut
	using(RATKAISUT_SJ_SG)
	where SAAJA_LKM = 1
	and maksukuukausi >= 201901
	)

select
null as maakunta
,coalesce(sukupuoli,'kaikki') as sukupuoli
,coalesce(ikaryhma,'kaikki') as ikaryhma
,vuosi
,count(distinct case when ETUUS = 'Sairauspäiväraha' then ASIAKAS_ID end) as sairauspaivaraha
,count(distinct case when etuus = 'Tartuntatautipäiväraha' and (EPIDEMIATUKI <> 'Kyllä' or EPIDEMIATUKI is null) then ASIAKAS_ID end) as tartuntatautipaivaraha
,count(distinct case when EPIDEMIATUKI = 'Kyllä' then ASIAKAS_ID end) as epidemiatuki
from 
	(select
	saajat.asiakas_id
	, saajat.vuosi
	, asiakas.sukupuoli
	,case
		when ika < 25 then 'alle 25'
		when ika between 25 and 34 then '25-34'
		when ika between 35 and 44 then '35-44'
		when ika between 45 and 54 then '45-54'
		when ika > 54 then 'yli 54' end  as ikaryhma
	, etuus
	, epidemiatuki
	from saajat
	left join asiakas
	on saajat.asiakas_id=asiakas.asiakas_id and saajat.vuosi=asiakas.vuosi
	) as alikysely
	
group by cube(SUKUPUOLI, ikaryhma), vuosi
order by 
	SUKUPUOLI
	,case ikaryhma
		when 'kaikki' then '0'
		when 'alle 25' then '1'
		when 'yli 54' then '5'
	else ikaryhma end
	, vuosi
;"

data2 <- get_query(sql_query) %>% 
  janitor::clean_names()


sprahat <- bind_rows(data1, data2)

# väliaikainen epidemiakorvaus (toimeentulotuen lisä syksyllä 2020)

sql_query <- "select 
asiakas_id
, maksukuukausi
, (case when valiaikainen_epidemiakorvaus='Kyllä' then 1 else 0 end) as korvaussaaja
, saaja_lkm
from DMPROD00.KADMIN.V_AINEISTO_F_SAAJAT_PC
where 1=1 
and maksukuukausi between 202010 and 202012
order by 1,2"

epidemiko <- get_query(sql_query) %>% 
  janitor::clean_names()

korvaussaajat_kk <- epidemiko %>% 
  group_by(maksukuukausi) %>% 
  summarise(totusaajat=sum(saaja_lkm),
            korvaussaajat=sum(korvaussaaja),
            molempiensaajat=sum(if_else(saaja_lkm==1 & korvaussaaja==1, 1, 0)),
            osuus_totusaajista=round(molempiensaajat/totusaajat*100,1)
  ) %>% 
  ungroup()

datalst <- list(
  'asumistuki' = asumistuki,
  'data1' = data1,
  'data2' = data2,
  'epidemiko' = epidemiko,
  'korvaussaajat_kk' = korvaussaajat_kk,
  'sprahat' = sprahat,
  'sql_query' = sql_query,
  'toimeentulotuki' = toimeentulotuki,
  'tyottomyysturva' = tyottomyysturva
)
saveRDS(datalst, "./datalst.RDS")
  
}
datalst <- readRDS("./datalst.RDS")


