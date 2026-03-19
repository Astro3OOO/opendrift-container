# Konfigurācijas fails

Visām apakšminētām configirācijas atribūtām jābūt apkopotiem viena vienotā JSON failā, piemēram kā: [config.json](INPUT/input_test.json).

- **OBLIGĀTIE**
    - *model* - modeļu veids, viens no dotiem: OceanDrift, Leeway, ShipDrift vai OpenOil. [`str`]
    - *start_position* - sākuma pozicijas koordinātes. Saraksts ar garumu 2, kur pirmā vietā ir `Latitude` un otrā `Longitude`. Garumam un platumam var būt gan `float` gan sraksti ar `float`, tomēr ir obligāti, lai izmēri sarakstiem sakrīt. Pie tam, ja turpmāk ir izvēlets `"seed_type" = "cone"`, tad obligāti lai katra coordinate sastāv tieši no divām vertībam (līnija sākumpunkts un beigu punkts). [`list`] 
    - *start_t* - sakuma laiks, kas ir ielasmas ar `pandas.to_datetime`. piemēram : `2025-12-08 11:00:00`. [`str`]
    - *end_t* - beigu laiks, kas ir ielasmas ar `pandas.to_datetime`. piemēram : `2025-12-31 12:00:00`. [`str`]
- **DATA RELATED**
    - *vocabulary* - vārdnīca, kur parametra vārdam no pievienota datatseta tiek piekārtots atbilstošais standarta CF nosaukums. Peejamsas vērtības ir [`Copernicus`, `ECMWF`], abām ir vēja apzīmējumi no ECMWF. Vārdnīca: [VariableMapping](DATA/VariableMapping.json) [`dict`]
    - *folder* - pēc nokulsējumja tas ir '/DATASETS'. Tas ir konteinera iekšēja mape, kas veidojas palaišanas laikā. Tai talāk tiek piemantota jebukra lokāla hosta mape. Mapei ir jāsastāv no `GRIB` vai `NetCDF` failiem, kas nav atsevišķ jānorada. [`str`]
        - *concatenation* - pēc izvēles, var piemantot mapi ar apakšmapēm un ieslēgt doto opciju. Pieņiem vertības `True` vai `False`, pēc noklusējuma ir `False`. Piemēram, gadījuma ja ir jāpalaiž ilga simulācija (vairāk par vienu vidēji ilgo prognozes ranu), tad var sadalīt visas lidzīgas prognozes pa apakšmapēm, un sakombinēt tos. Piemēram, sadalīt mapēs : wave-model, atmospheric-model. Tad ar šo opciju datu faili no katras mapes būs sašūti kopā pa vienu datasetu atbilstoši katrai mapei. [`bool`]  
    - *selection* - automatiskā failu izvelēšana no dota *folder* attiecīgi ievadītajām laika intervālam. Pēc noklusējuma izslēgts ar `False`, lai ieslēgtu jānomaina un `True`. Kad automatiskā failu izvelēšana ir ieslēgta, iedota mape tiek skanēta uz struktūru. Ja mape sastāv no apakšmapēm (piemēram: phys/wave/atmo), tad tiek izvelēti vajadzīgie faili no katras apakšmapes. Ja galvenā mape sastāv no failiem, tad sākumā tiek izvelēti vajadzīgie faili un tad ir konstruētas apakšmapes pēc katra prognozes veida. Izvelētie faili ir novirzīti uz konteinera mapi '/SELECTED' kur tie ir definēti ar simboliskajiem linkiem. [`bool`]
- **SIMULĀCIJAS**
    - *num* - simulēto daļiņu skaits. Tam jābūt veselam pozitīvam skaitlim. Pēc noklusējuma tas ir 100. [`int`]
    - *seed_type* - ir pieejami divi punktu izvietošnas veidi: 'elements', 'cone' un 'shapefile'. Opcija 'shapefile' ir domāta OpenOil modelim. Tas prasa shapefaila piemountošanu kontenerim. Piemountotam failam jābūt lasāmām ar [Geopandas](https://geopandas.org/en/stable/), tapēc ir ieteikts nodot to `.geojson` formatā.  Pēc noklusējuma tas ir 'elemnets', kas sēj daļiņas ka atsevišķus punktus. [`str`]
    - *rad* - punktu dispersijas rādiuss apkārt izvēlēt sākumpunkta. Ja ir izvēlēts 'elemnts' ka *seed_type* parametrs, tad radiuss var būt vai no vesels pozitīvs skaitlis, vai saraksts ar garumu vienādu ar `Latitude` un `Longitude` sarakstu garumiem. Ja ir izvēlēts 'cone', tad radiuss var būt vai nu viens pozitīvs vesels skaitlis, vai srakasts ar dieviem skaitļiem. Piemērma konuss ar rad = [0, 1000] izviedo sākuma punktu kopu, kur pie pirmā pinktu būs daļiņu izklēdie 0m un pie pedēja izklēde būs 1000m. Pēc noklusējuma vērtība radiusam ir 0 metri. [`int`] vai [`list`] ar [`int`]. 
    - *backtracking* - var pieslēgt šo opciju ar `True` vērtību, bet tad ***OBLIGĀTI*** sākuma laikam jābūt lielākam par beigu laiku un *time_step* juābūt negatīvam. Pēc noklusējuma šī opcija ir izslēgta. [`bool`]
    - *time_step* - var noradīt simulācijas laiak soli sekundēs. Skaitļim jābūs veselam. Pēc noklusējuma, tas ir 1800 sekundes (30 min), bet var palielināt un samazināt. Ir atļauta negatīva vertība, tikai ja ir ieslēgts *backtracking* ar `True` vēretību un sākuma laiks ir pirms beigu laika. [`int`]
- **MODĒĻU IESTATĪJUMI**
    - *wdf* - vēja dreifa faktors, kas ir nosakošais parametrs OceanDrift modelim. Tam jābūt intervālā no 0 līdz 1. Pēc nokjlusējuma tas ir 0.02 jeb 2%, kas nozīmē, ka objekts parvietojas ar 2% ātrumu no vēja atruma. [`float`]
    - *lw_obj* - Leeway objektu numurs, no 1 līdz 85. [Leeway objektu saraksts](https://github.com/OpenDrift/opendrift/blob/master/opendrift/models/OBJECTPROP.DAT). Pēc noklusējuma tas ir 1. [`int`]
    - *ship* - nosakošais parametrs priekš ShipDrift modeļa. Tas ir 4 vērtību saraksts ar kuģu izmēriem [length, beam, height, draft] metros, pēc noklusējuma tas ir [62, 8, 10, 5]. [`list`]
        - *orientation* - kuģu priekšejas daļas orientācija pret vēju. Var būt 'left', 'right' un 'random'. Pēc noklusejuma tas ir 'random', kas nozīme, ka use no objektiem būs ar kreiso un puse būs ar labo sāni pret vēju. [`str`]
    - *oil_type* - Naftas produkta standartizēts nosaukums. [Nosaukumu saraksts](https://adios.orr.noaa.gov/oils) [Excel saraksts ar nosaukumiem](https://lvgmc.sharepoint.com/:x:/s/KSMN/IQCwtRzJHfH-QJk_J9iTLO2RAUri3_0D3OOroeiIFqNQtAk?e=L5WPIg) [`str`]
- **PAPILDUS**
    - *configurations* - var pievienot papildus simulācijas konfigurācijas no [saraksta](https://lvgmc.sharepoint.com/:x:/s/KSMN/IQCL8Fl45boXSbFMqqSm7mWGAXYaslD0hSFFY1kOkYhtdfU?e=grtsTH). [`dict`]
    - *file_name* - var pievienot *output* faila nosaukumu. Ja nav noradīts, tad tas tiek ģenerēts automātiski: '{model}_{start_time}_{now_time}.nc'. [`str`]
    - *prerun* - var ieslēgt sākuma simulāciju ar konstantun vēju un straumi. Šī opcija papildus prasa parametrus *duration* un *forcings*. Šī funkcionalitāte ir paredzēta manuālai novērojumu ievadei faktiskajos laikapstākļos. Pēc īslaicīgas simulācijas beigām, tas beigu stāvoklis (laiks un pozīcija) tiks padots ka sākuma stavoklis pilnvertīgai simulācijai kas turpināsises līdz *end_t*. [`bool`] 
        - *duration* - simulācijas ilgums teksta formā, piemēram: `1hour 23minutes 54seconds` vai `01:23:54`. [`str`]
        - *forcings* - [windir, windspeed, currentdir, currentspeed] - saraksts ar 4 skaitļiem, kas reprezentē faktiskus laikapstākļus novērojumu vietā. [`list`]
    - *allow_empty_ds* - DEBUGGING variable. Netiek lietots simulācijās, ir domats konteinera testiem kad netiek nodoti dati. Pēc noklusējuma ir `False`, tāde veidā aizliedzot palaist simulaciju bez datiem. [`bool`]
    - *postprocessing* - var izvelēties, kā apstradāt trajektorijas failu pēc simulācijas pabeigšanas. [`dict`] Pēc noklusējuma tas ir izslegts, bet var ieslegt ar sekojošam atslēgam:
        - *POC* - atgriez `.geojson` failu ar taisnstūru multipoligoniem, kur krāsa norāda uz dota reģiona objekta saturešanas vārbutību. [Krāsu skala](pallets/POC_scale.drawio.png) [`bool`] 
        <!-- - *Triangle* - atgriež `.geojson` failu ar trajektorijas trīssturi. [`bool`] -->
        - *ConvexHull* - atgriež `.geojson` failu ar minimālo poligonu, kas parklāj visus punktus. [`bool`]
        - *Picture* - atgriež trajektorijas bildi `.png` formatā. [`bool`]