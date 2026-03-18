# Dreifa modelēšanas rīks

Konteineris ir paradzēts jūras objektu dreifa simulācijas palaišanai. Projekts ir veidots uz [OpenDrift](https://opendrift.github.io/) **Pyhton** bibliotēkas bāzes. Ka *input* tas pieņiem JSON konfigurācijas failu un datasetus GRIB vai NetCDF formatā. *output* ir datasets ar kustības trajektoriju, kas ir saglabāta NetCDF formatā.

# Projekta struktūra
```
opendrift-container/
│
├── main.py                     # pamata programma
├── src/
│   ├── config_verification.py      # JSON faila validācija un sadalīšana uz simulācijas un datu konfigurācijam
│   ├── case_study_tool.py          # simulācijas funkcijas
│   ├── general_tools.py     		# Rīki, kurus lieto vairāki moduli
│   ├── file_clusterization.py      # Rīks, lai sadalītu falus apakšmapēs atbilstoši unikāliem nosaukumiem failu nosaukumā (lietots iekš dataset_selection.py)
│   ├── post_processing.py     		# gatavas trajektorijas pēcapstrāde
│   └── dataset_tools/
│       ├── dataset_verification.py     # Datasetu validācija. Pārbauda vai ievadītais laiks pārklājās as datu laikiem
│       ├── dataset_selection.py        # Datasetu automatizēta izvelēšana atkarība no pieprasīta laika. 
│       └── dataset_preparation.py      # Ielasa datasetus un sagatavo tos lietojumam simulācijā
│
├── DATA/
│   ├── VariableMapping.json    # Iekšeja vārdnīca priekš korektu parametru nosaukumu ielasīšanās
│   └── colorscale.json			# Krasu skalas līmeņi un hex krāsu kodi, atbilstosi katram līmenim. 
│
├── INPUT/                      
│   └── input_test.json			# fitktīvais konfigurācijas fails priekš conteinera testa
│
├── tests/                     
│   └── test_functions.py		# galveno funkciju testi  
├── pallets/             
│   ├── POC_scale.drawio.png		    # krasu skala POC kartēm
│   └── POC_scale.drawio_dark.png		# ta pati krasu skala tikai tumšas tēmas stilā  
│
├── requirements.txt            # Python nepieciešamas paketes
├── Dockerfile                  # Konteinera iestatījumi
├── .dockerignore               # faili, kurus konteineris ignorēs
├── .gitignore                  # faili, kurus Git ignorēs
└── README.md    
```
# Setup & Usage
- Image izveidošana:

```docker build -t opendrift-container .```

- Konteinera palaišana:

```
docker run \
	-v path/to/host/dataset/folder:/DATASETS \
   	-v path/to/host/config/file.json:/opendrift-container/INPUT/config.json \
	-v path/to/store/results:/OUTPUT \
	opendrift-container python main.py config.json 
``` 
# Workflow design
```mermaid
flowchart TD;
	A[Start main.py] --> B[Get raw config file];
	B --> C[Resolve path];
	C --> D{Path exists?};
	D -- No --> Q1[Exit code 1 or 2];
	D -- Yes ---> Q2[Verify config file];
	Q2 --> E{Is config valid ?};
	E -- No --> Q3[Exit code 3];
	E -- Yes ---> Q4[Get vocabulary];
	Q4 --> F{Exist and readable ?};
	F -- No --> Q5[Exit code 4 or 5];
	F -- Yes ---> Q6{Select dataset};
	Q6 -- True --> Q7[Try to select ans symlink Dataset];
	Q7 --> Q8{Success?};
	Q8 -- No --> Q9[Exit code 10]
	Q8 -- Yes ---> G;
	Q6 -- False ----> G[Inspect vocabulary];
	G --> G1{Is presented in /DATA ?}
	G1 -- False --> G2[Exit code 7];
	G1 -- True ---> H[Prepare Dataset];
	H --> H1{Success?};
	H1 -- No --> H2[Exit code 6];
	H1 -- Yes ---> I[Validate Dataset];
	I --> I1{Valid?};
	I1 -- No --> I2[Exit code 8];
	I1 -- Yes ---> J{Shape file allowed?};
	J -- Yes --> J1{Shapefile valid?};
	J1 -- False --> J2[Exit code 12, 13, or 14];
	J1 -- True ---> K[Run simulation];
	J -- No --> K;
	K --> K1{Success?};
	K1 -- No --> K2[Exit code 9];
	K1 -- Yes ---> L{Post processing ?};
	L -- Yes --> L1[Make post processing];
	L1 --> L2{Success?};
	L2 -- No --> L3[Exit code 11];
	L2 -- Yes ---> Z;  
	L -- No ---> Z[Finish];
	Z --> END[Exit code 0];
classDef exit fill:#f88,stroke:#000,stroke-width:1px;
classDef green fill:#9F7,stroke:#333,stroke-width:2px;
class Q1,Q3,Q5,Q9,G2,H2,I2,J2,K2,L3 exit;
class END green 
```