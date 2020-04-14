package covid;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.opencsv.CSVIterator;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.Charsets;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.hl7.fhir.r4.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;


@SpringBootApplication
public class UKCovidExtractApp implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(UKCovidExtractApp.class);

    private class NHSStat {
        String org;

        Date date;

        Integer maleTriage = 0;
        Integer femaleTriage = 0;
        Integer unknownTriage = 0;

        Integer maleOnline = 0;
        Integer femaleOnline = 0;
        Integer unknownOnline = 0;

        Double covidRiskFactor =Double.valueOf(0);
        Double nhsCostEstimate = Double.valueOf(0);
    }



    String phe;
    String uec;
    String morbidity;

    final String UUID_Prefix = "urn:uuid:";

    final String ONSSystem = "https://fhir.gov.uk/Identifier/ONS";

    final int batchSize = 10;

    FhirContext ctxFHIR = FhirContext.forR4();

    private ArrayList<MeasureReport> reports = new ArrayList<>();

    private Map<String,Location> locations = new HashMap<>();

    private Map<String,String> missinglocation = new HashMap<>();

    private Map<String, BigDecimal> hi = new HashMap<>();
    private Map<String, BigDecimal> mdi = new HashMap<>();
    private Map<String, BigDecimal> population = new HashMap<>();

    private Map<String, Map<Date, NHSStat>> nhs = new HashMap<>();
    private Map<String, Map<Date, NHSStat>> nhsParent = new HashMap<>();
    private Map<String, Map<String, Integer>> ccgPopulation = new HashMap<>();

    ClassLoader classLoader = getClass().getClassLoader();

    public static void main(String[] args) {
        SpringApplication.run(UKCovidExtractApp.class, args);
    }

    String PHE_UACASES_URL = "https://www.arcgis.com/sharing/rest/content/items/b684319181f94875a6879bbc833ca3a6/data";
    String PHE_DAILYINDICATORS_URL = "https://www.arcgis.com/sharing/rest/content/items/bc8ee90225644ef7a6f4dd1b13ea1d67/data";
    String PHE_EXCEL = "https://fingertips.phe.org.uk/documents/Historic%20COVID-19%20Dashboard%20Data.xlsx";


    String NHS_PATHWAYS_URL = "https://files.digital.nhs.uk/08/510910/NHS%20Pathways%20Covid-19%20data%202020-04-07.csv";
    String NHSONLINE_URL = "https://files.digital.nhs.uk/EA/9901C2/111%20Online%20Covid-19%20data_2020-04-07.csv";

    DateFormat dateStamp = new SimpleDateFormat("yyyy-MM-dd");

    DateFormat hisFormat = new SimpleDateFormat("dd/MM/yyyy");

    DateFormat stamp = new SimpleDateFormat("yyyyMMdd");


    IGenericClient client = ctxFHIR.newRestfulGenericClient("https://fhir.test.xgenome.co.uk/R4");

    @Override
    public void run(String... args) throws Exception {
        if (args.length > 0 && args[0].equals("exitcode")) {
            throw new Exception();
        }
     ;

        SetupMeasures();

        ProcessDeprivation();
        SetupPopulations();

        SetupPHELocations();


        ProcessPHEMorbidityDailyIndicators();
        // Getting narked with this data. So inconsistent need to remove
        ProcessPHEDailyUAFile();
        ProcessPHEMorbidity();
        ProcessPHEExcelFile();

        SetupNHSLocations();
        PopulateNHS();

      //  if (true) throw new InternalError("EOF");



    }

    private void FixLocations(){

        for(String location : locations.keySet()) {
            log.info("Removing reports for {}",location);
            RemoveOrgReport(location);
            /*
            if (!location.equals(GetMergedId(location))) {
                log.info("Removing reports for {}",location);
                RemoveOrgReport(location);
            }

             */
        }

    }

    private void SetupMeasures(){
        Measure measure = new Measure();
        measure.addIdentifier().setSystem("https://fhir.mayfield-is.co.uk/MEASURCODE").setValue("PHE_COVID");
        measure.setStatus(Enumerations.PublicationStatus.ACTIVE);
        MethodOutcome outcome = client.create().resource(measure).conditionalByUrl("Measure?identifier=https://fhir.mayfield-is.co.uk/MEASURCODE|PHE_COVID").execute();
        phe = "Measure/"+outcome.getId().getIdPart();
        measure = new Measure();
        measure.addIdentifier().setSystem("https://fhir.mayfield-is.co.uk/MEASURCODE").setValue("UEC_COVID");
        measure.setStatus(Enumerations.PublicationStatus.ACTIVE);
        outcome = client.create().resource(measure).conditionalByUrl("Measure?identifier=https://fhir.mayfield-is.co.uk/MEASURCODE|UEC_COVID").execute();
        uec= "Measure/"+outcome.getId().getIdPart();
        measure = new Measure();
        measure.addIdentifier().setSystem("https://fhir.mayfield-is.co.uk/MEASURCODE").setValue("MORBIDITY_COVID");
        measure.setStatus(Enumerations.PublicationStatus.ACTIVE);
        outcome = client.create().resource(measure).conditionalByUrl("Measure?identifier=https://fhir.mayfield-is.co.uk/MEASURCODE|MORBIDITY_COVID").execute();
        morbidity= "Measure/"+outcome.getId().getIdPart();
    }

    private void SetupPopulations() throws Exception {
        // Population

        log.info("Processing Population");
        ProcessPopulationsFile("UK.csv");
        ProcessPopulationsFile("EnglandRegions.csv");
        ProcessPopulationsFile("LocalAuthority.csv");
        ProcessPopulationsFile("2019-ccg-estimates.csv");
    }

    private void SetupPHELocations() throws Exception {
        // Locations
        log.info("Processing PHE Locations");
        ProcessLocationsFile("Z92_UK.csv","CTRY");
        ProcessLocationsFile("E92_CTRY.csv","CTRY");
        ProcessLocationsFile("E12_RGN.csv","RGN");
       // FixLocations();

        ProcessLocationsFile("E11_MCTY.csv","MCTY");
        ProcessLocationsFile("E10_CTY.csv","CTY");
        ProcessLocationsFile("E09_LONB.csv","LONB");
        ProcessLocationsFile("E08_MD.csv","MD");
        ProcessLocationsFile("E07_NMD.csv","NMD");
        ProcessLocationsFile("E06_UA.csv","UA");
        // Not required   ProcessLocationsFile("E05_WD.csv","WD");


    }
    private void SetupNHSLocations() throws Exception {
        // Locations
        log.info("Processing NHS Locations");

        // These next three files have been modified to make sense
        ProcessLocationsFile("E40_ENG.csv","NHSENG");
        ProcessLocationsFile("E40_NHSER.csv","NHSER");
        // This is the main top level codes
        ProcessLocationsFile("E_OTHER.csv","NHSOTHER");
        ProcessLocationsFile("E39_NHSRLO.csv","NHSRLO");

        ProcessLocationsFile("E38_CCG.csv","CCG");
        // This will recalculate
        ProcessCCGPopulationEstimates();

        ProcessLocationsFile("E_UNK.csv","NHS_OTHERREGION");

        ProcessLocationsFile("E12_RGN.csv","NHSRGN");
        ProcessLocationsFile("E06_UA.csv","NHSUA");
    }

    private void ProcessPHEMorbidityDailyIndicators() throws Exception {

        reports = new ArrayList<>();

        BufferedInputStream zis = new BufferedInputStream(new URL(PHE_DAILYINDICATORS_URL).openStream());
        Workbook wb = new XSSFWorkbook(zis);
        Sheet  sheet = wb.getSheet("Sheet1");
        if (sheet != null) {
            Row header = sheet.getRow(0);

                Row row = sheet.getRow(1);
                for(int f=1;f < row.getLastCellNum();f++) {
                    String onsCode = null;
                    if (header.getCell(f) != null && header.getCell(f).getCellType() != null) {
                        switch (header.getCell(f).getStringCellValue()) {
                            case "TotalUKDeaths":
                                onsCode = "Z92";
                                break;
                            case "EnglandDeaths":
                                onsCode = "E92000001";
                                break;
                            case "ScotlandDeaths":
                                onsCode = "S92000003";
                                break;
                            case "WalesDeaths":
                                onsCode = "W92000004";
                                break;
                            case "NIDeaths":
                                onsCode = "N92000002";
                                break;
                        }
                        if (onsCode != null) {
                            Date columnDate = null;
                            try {
                                columnDate = row.getCell(0).getDateCellValue();

                                MeasureReport report = getMorbidityMeasureReport(Date.from(columnDate.toInstant()),
                                        (int) row.getCell(f).getNumericCellValue(),
                                        onsCode);
                                if (report != null) this.reports.add(report);
                            } catch (Exception ex) {
                                log.info("OnsCode {} Row Number {} Cell NUmber {}", onsCode, 1, f);
                                log.info("columnDate {}", columnDate);

                                throw ex;
                            }
                        }
                    }

            }


            UploadReports();

        }


    }

    private void ProcessPHEMorbidity() throws Exception {

        reports = new ArrayList<>();

        BufferedInputStream zis = new BufferedInputStream(new URL(PHE_EXCEL).openStream());
        Workbook wb = new XSSFWorkbook(zis);
        Sheet  sheet = wb.getSheet("UK Deaths");
        if (sheet != null) {
            Row header = sheet.getRow(7);
            for(int i =8;i < sheet.getLastRowNum(); i++ ) {
                Row row = sheet.getRow(i);
                for(int f=2;f < row.getLastCellNum();f++) {
                    String onsCode;
                    if (header.getCell(f) != null && header.getCell(f).getCellType() != null) {
                        switch (header.getCell(f).getStringCellValue()) {
                            case "UK":
                                onsCode = "Z92";
                                break;
                            case "England":
                                onsCode = "E92000001";
                                break;
                            case "Scotland":
                                onsCode = "S92000003";
                                break;
                            case "Wales":
                                onsCode = "W92000004";
                                break;
                            case "Northern Ireland":
                                onsCode = "N92000002";
                                break;
                            default:
                                throw new InternalError("Missing country code " + header.getCell(f).getStringCellValue());

                        }

                        Date columnDate = null;
                        try {
                            columnDate = row.getCell(0).getDateCellValue();

                            MeasureReport report = getMorbidityMeasureReport(Date.from(columnDate.toInstant()),
                                    (int) row.getCell(f).getNumericCellValue(),
                                    onsCode);
                            if (report != null) this.reports.add(report);
                        } catch (Exception ex) {
                            log.info("OnsCode {} Row Number {} Cell NUmber {}", onsCode, i, f);
                            log.info("columnDate {}", columnDate);

                            throw ex;
                        }
                    }
                }
            }


            UploadReports();

        }


    }

    private void ProcessCCGPopulationEstimates() throws Exception {



            InputStream zis = classLoader.getResourceAsStream("CCGEstimates2018.xls");
            Workbook wb = new HSSFWorkbook(zis);
            Sheet  sheet = wb.getSheet("Persons");
            if (sheet != null) {
                Row header = sheet.getRow(6);
                for(int i =7;i < sheet.getLastRowNum(); i++ ) {
                    Row row = sheet.getRow(i);
                    String oldCode = row.getCell(0).getStringCellValue();
                    // need to obtain new code
                    String onsCode = GetMergedId(oldCode);
                    String group = row.getCell(2).getStringCellValue();

                    Integer count = (int) row.getCell(3).getNumericCellValue();
                    if (onsCode != null && !onsCode.isEmpty() && group !=null && !group.isEmpty())  {
                        group = getBand(group);
                        if (this.ccgPopulation.get(onsCode) == null) {
                            this.ccgPopulation.put(onsCode, new HashedMap());
                        }
                        Map<String, Integer> bands = this.ccgPopulation.get(onsCode);
                        if (bands.get(group) == null) {
                            bands.put(group,count);
                        } else {
                            bands.replace(group, bands.get(group) + count);
                        }
                    }
                }


            }
    }
    private String getBand(String group) throws Exception {
        switch (group){

            case "0-4":
            case  "5-9":
                return "0-9 years old";
            case  "10-14":
            case  "15-19":
                return "10-99 years old";
            case  "20-24":
            case  "25-29":
                return "20-29 years old";
            case  "30-34":
            case  "35-39":
                return "30-39 years old";
            case  "40-44":
            case  "45-49":
                return "40-49 years old";
            case  "50-54":
            case  "55-59":
                return "50-59 years old";
            case  "60-64":
            case  "65-69":
                return "60-69 years old";
            case  "70-74":
            case  "75-79":
                return "70-79 years old";
            case  "80-84":
            case  "85-89":
            case  "90+":
                return "80+ years old";
            case  "All ages":
                return "All";
            default:
                throw new Exception("unmapped age band");
        }
    }

    private String GetMergedId(String oldId) {

        // Source https://digital.nhs.uk/services/organisation-data-service/change-summary---stp-reconfiguration
        switch (oldId) {

            case "E38000058":
            case "E38000071":
            case "E38000115":
            case "E38000169":
                return "E38000229"; // Derbys this was a 2019 change

            case "E38000129":
            case "E38000152":
                return "E38000230"; // Devon

            // Jorvik
            case "E38000018" :
            case "E38000019":
            case "E38000001":
                return "E38000232"; //Bradford
            case "E38000073":
            case "E38000145":
            case "E38000069":
                return "E38000241"; // North Yorkshire

            case "E38000162":
            case "E38000075":
            case "E38000042":
                return "E38000247"; // Tees Valley
            case "E38000116":
            case "E38000047":
                return "E38000234"; // County Durham
            case "E38000056":
            case "E38000151":
            case "E38000189":
            case "E38000196":
                return "E38000233"; // north cheshire

            case "E38000078":
            case "E38000139":
            case "E38000166":
            case "E38000211":
                return "E38000236"; // Hereford and Worcs

            case "E38000037":
            case "E38000108":
                return "E38000242"; // Norhamptomshire


            case "E38000103":
            case "E38000109":
            case "E38000132":
            case "E38000133":
            case "E38000134":
            case "E38000142":
                return "E38000243"; // Notts

            case "E38000099":
            case "E38000100":
            case "E38000157":
            case "E38000165":
                return "E38000238"; // Lincs


            case "E38000063":
            case "E38000124":
            case "E38000203":
            case "E38000219":
            case "E3800013":
                return "E38000239"; //NHS Norfolk & Waveney CCG

            case "E38000011":
            case "E38000023":
            case "E38000066":
            case "E38000092":
            case "E38000098":
            case "E38000171":
                return "E38000244"; // NHS South East London CCG

            case "E38000040":
            case "E38000090":
            case "E38000140":
            case "E38000105":
            case "E38000193":
            case "E38000179":
                return "E38000245"; // NHS South West London CCG


            case "E38000005":
            case "E38000027":
            case "E38000057":
            case "E38000072":
            case "E38000088":
                return "E38000240"; //NHS North Central London CCG

            case "E38000002":
            case "E38000029":
            case "E38000043":
            case "E38000104":
            case "E38000156":
            case "E38000180":
            case "E38000184":
            case "E38000199":
                return "E38000237"; //NHS Kent and Medway CCG

            case "E38000067":
            case "E38000128":
            case "E38000177":
            case "E38000054":
                return "E38000246"; //NHS Surrey Heartlands CCG

            case "E38000213":
            case "E38000039":
            case "E38000083":
                return "E38000248"; // NHS West Sussex CCG

            case "E38000076":
            case "E38000081":
            case "E38000055":
                return "E38000235"; // NHS East Sussex CCG

            case "E38000009":
            case "E38000181":
            case "E38000206":
                return "E38000231"; // NHS Bath and North East Somerset, Swindon and Wiltshire CCG
            default:
                return oldId;
        }

    }

    private void UploadReports() throws Exception {
        Bundle bundle = null;

        int count = 0;
        int fileCnt=0;

        for (MeasureReport measureReport : this.reports) {

            if ((count % batchSize) == 0) {

                if (bundle != null) sendMeasures(bundle, fileCnt);
                bundle = new Bundle();
                bundle.getIdentifier().setSystem("https://fhir.mayfield-is.co.uk/Id/")
                        .setValue(UUID.randomUUID().toString());
                bundle.setType(Bundle.BundleType.TRANSACTION);
                fileCnt++;
            }
            Bundle.BundleEntryComponent entry = bundle.addEntry()
                    .setFullUrl(UUID_Prefix + UUID.randomUUID().toString())
                    .setResource(measureReport);
            String conditionalUrl = getConditional(measureReport.getIdentifierFirstRep());
            entry.getRequest()
                    .setMethod(Bundle.HTTPVerb.PUT)
                    .setUrl(entry.getResource().getClass().getSimpleName() + "?" + conditionalUrl);
            count++;
        }
        if (bundle != null && bundle.getEntry().size() > 0) {
            sendMeasures(bundle, fileCnt);
        }

    }

    private void PopulateNHS() throws Exception {

        reports = new ArrayList<>();
        // https://digital.nhs.uk/data-and-information/publications/statistical/mi-potential-covid-19-symptoms-reported-through-nhs-pathways-and-111-online/latest
        GetNHSTriageData( NHS_PATHWAYS_URL );
        GetNHSOnlineData(NHSONLINE_URL);

        CostEstimate();
        // Build entries for top level codes
        nhsParent = new HashMap<>();

        for (Map.Entry<String, Map<Date, NHSStat>> en : nhs.entrySet()) {
            for (Map.Entry<Date, NHSStat> entry : en.getValue().entrySet()) {
                populateParent( entry.getValue());
            }
        }
        this.nhs.putAll(nhsParent);


        if (missinglocation.size()>0) {
            ProcessMissingLocation();
          //  throw new InternalError("Missing data");
        }

        CalculateNHSRegional();

        UploadReports();
    }

    private void CostEstimate() {
        //  Cost Estimate
        for (Map.Entry<String, Map<Date, NHSStat>> en : nhs.entrySet()) {
            List<Double> costs = new ArrayList<Double>();
            Map<String, Integer> popMap = this.ccgPopulation.get(en.getKey());
            if (popMap != null) {
                Map<Date, NHSStat> treeMap = new TreeMap(en.getValue());

                for (Map.Entry<Date, NHSStat> entry : treeMap.entrySet()) {
                    NHSStat stat = entry.getValue();
                    Double population = Double.valueOf(popMap.get("All"));
                    Double rawCost = Double.valueOf(popMap.get("0-9 years old")) * 0;

                    rawCost += popMap.get("10-99 years old") * 0.002;
                    rawCost += popMap.get("20-29 years old") * 0.002;
                    rawCost += popMap.get("30-39 years old") * 0.002;

                    rawCost += popMap.get("40-49 years old") * 0.004;

                    rawCost += popMap.get("50-59 years old") * 0.013;

                    rawCost += popMap.get("60-69 years old") * 0.036;

                    rawCost += popMap.get("70-79 years old") * 0.08;

                    rawCost += popMap.get("80+ years old") * 0.148;

                    Double cost = rawCost / population;
                    stat.covidRiskFactor = cost;
                    cost = cost * (stat.maleTriage + stat.femaleTriage +stat.unknownTriage );

                    costs.add(cost);
                    Double avg = Double.valueOf(0);
                    for (int f=0; f<costs.size();f++) {
                        avg += costs.get(f);
                    }
                    stat.nhsCostEstimate = avg/costs.size();
                    if (costs.size()>8) {
                        List<Double> tempcosts =costs.subList(1, costs.size());
                        costs=tempcosts;
                    }

                //    log.info("location {} population {} cost estimate = {}",  en.getKey(), popMap.get("All"), cost);
                }
            }

        }
    }


    private void ProcessMissingLocation() throws Exception {
        File file = new File("MissingLocation.csv");

            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter
                    writer = new CSVWriter(outputfile);
            // adding header to csv
            String[] header = { "GEOGCD", "GEOGNM" };
            writer.writeNext(header);

            for (Map.Entry<String, String> location : missinglocation.entrySet()) {
                String[] data = { location.getKey(), location.getValue()};
                writer.writeNext(data);
            }

            // closing writer connection
            writer.close();


    }

    private void CalculateNHSRegional() {
        for (Map.Entry<String, Map<Date, NHSStat>> org : nhs.entrySet()) {
            int maleTriageTotal =0;
            int femaleTriageTotal =0;
            int unknownTriageTotal =0;
            int maleOnlineTotal =0;
            int femaleOnlineTotal =0;
            int unknownOnlineTotal =0;

            Map<Date, NHSStat> treeMap = new TreeMap(org.getValue());

            for (Map.Entry<Date, NHSStat> dateentry : treeMap.entrySet()) {
                //System.out.println("Key = " + dateentry.getKey());
                NHSStat nhs = dateentry.getValue();

                MeasureReport report = new MeasureReport();
                report.addIdentifier()
                        .setSystem("https://fhir.mayfield-is.co.uk/Measure/NHS111")
                        .setValue(org.getKey() + "-" + stamp.format(dateentry.getKey()));

                report.setDate(dateentry.getKey());
                report.setPeriod(new Period().setStart(dateentry.getKey()));
                report.setStatus(MeasureReport.MeasureReportStatus.COMPLETE);
                report.setType(MeasureReport.MeasureReportType.SUMMARY);
                report.setReporter(new Reference().setIdentifier(new Identifier().setSystem(ONSSystem).setValue(org.getKey())));
                report.setSubject(new Reference().setIdentifier(new Identifier().setSystem(ONSSystem).setValue(org.getKey())));

                report.setMeasure(uec);

                Location location = locations.get(org.getKey());
                if (location != null) {
                    report.getSubject().setReference(location.getId());
                    report.getSubject().setDisplay(location.getName());
                    report.getReporter().setReference(location.getId());
                    report.getReporter().setDisplay(location.getName());
                } else {
                    throw new InternalError("Missing Location Code");
                }

               maleTriageTotal += nhs.maleTriage;
                femaleTriageTotal += nhs.femaleTriage;
                unknownTriageTotal += nhs.unknownTriage;

                femaleOnlineTotal += nhs.femaleOnline;
                maleOnlineTotal += nhs.maleOnline;
                unknownOnlineTotal += nhs.unknownOnline;

                BigDecimal population = this.population.get(report.getSubject().getIdentifier().getValue());

                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","nhs-cost","Cost Estimate", nhs.nhsCostEstimate,null);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","risk-factor","Risk Factor", nhs.covidRiskFactor,null);

                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","daily-triage","UEC Daily Total", Double.valueOf(nhs.femaleTriage+nhs.maleTriage+nhs.unknownTriage),null);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","daily-online","NHS 111 Website Daily Total", Double.valueOf(nhs.femaleOnline+nhs.maleOnline+nhs.unknownOnline),null);

                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male-online-total","UEC Male Total", Double.valueOf(maleOnlineTotal),null);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female-online-total","UEC Female Total", Double.valueOf(femaleOnlineTotal),null);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","online-total","COVID Symptoms Reported NHS 111 website", Double.valueOf(maleOnlineTotal + femaleOnlineTotal + unknownOnlineTotal),population);

                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male-triage-total","UEC Male Total", Double.valueOf(maleTriageTotal),null);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female-triage-total","UEC Female Total", Double.valueOf(femaleTriageTotal),null);
                addGroup(report,"http://snomed.info/sct","840544004","Suspected disease caused by severe acute respiratory coronavirus 2 (situation)", Double.valueOf(femaleTriageTotal + maleTriageTotal+ unknownTriageTotal),population);

             //   log.info("{} count {} {}", report.getIdentifierFirstRep().getValue(), nhs.femaleTriage + nhs.maleTriage, femaleTriageTotal + maleTriageTotal);

                reports.add(report);

            }
        }
    }

   private void  populateParent(  NHSStat nhs) {
        Location location = locations.get(nhs.org);
        if (location != null) {
            Location parent = locations.get(location.getPartOf().getIdentifier().getValue());
            if (parent != null) {

                Map<Date, NHSStat> nhsStat = nhsParent.get(parent.getIdentifierFirstRep().getValue());
                if (nhsStat == null) {
                    nhsStat = new HashMap<>();
                    nhsParent.put(parent.getIdentifierFirstRep().getValue(), nhsStat);
                }
                NHSStat parentStat = nhsStat.get(nhs.date);
                if (parentStat == null) {
                    parentStat = new NHSStat();
                    parentStat.org = parent.getIdentifierFirstRep().getValue();
                    parentStat.date = nhs.date;
                    nhsStat.put(nhs.date, parentStat);
                } else {
                    log.debug("Found past entry");
                }
                parentStat.femaleTriage += nhs.femaleTriage;
                parentStat.maleTriage += nhs.maleTriage;
                parentStat.unknownTriage += nhs.unknownTriage;
                parentStat.maleOnline += nhs.maleOnline;
                parentStat.femaleOnline += nhs.femaleOnline;
                parentStat.unknownOnline += nhs.unknownOnline;

                populateParent(parentStat);
            } else {
                if (!nhs.org.equals("E40000000")) {
                    throw new InternalError("Parent of " + nhs.org + " should be present");
                }
            }
        } else {
            throw new InternalError("Org should not be empty");
        }
    }
private void addGroup(MeasureReport report, String system, String code, String display, Double qtyValue, BigDecimal population ) {
    MeasureReport.MeasureReportGroupComponent group = report.addGroup();
    Quantity qty = new Quantity();
    BigDecimal value= new BigDecimal(qtyValue);
    qty.setValue(value);
    group.setMeasureScore(qty);

    if (population != null) {
        group.addPopulation().setCount(population.intValue());
    }
    group.setCode(new CodeableConcept().addCoding(
            new Coding().setSystem(system)
                    .setCode(code)
            .setDisplay(display)
    ));


}

    private void RemoveOrgReport(String org) {
        Bundle bundle = client.search().byUrl("MeasureReport?subject.identifier="+org).returnBundle(Bundle.class).execute();
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof MeasureReport) {
                log.debug("Delete {} " + ((MeasureReport) entry.getResource()).getIdElement() );
                client.delete().resourceById(((MeasureReport) entry.getResource()).getIdElement()).execute();
            }
        }

    }
    private void GetNHSTriageData(String fileUrl) throws Exception {
        BufferedInputStream zis = new BufferedInputStream(new URL(fileUrl).openStream());

            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader, ',', '\"', 0));
            String[] header = null;
            int count = 0;
            for (CSVIterator it = iterator; it.hasNext(); ) {

                if (count == 0) {
                    header = it.next();
                } else {
                    String[] nextLine = it.next();
                    String onsCode = GetMergedId(nextLine[4]);
                    Map<Date, NHSStat> ccg = nhs.get(onsCode);
                    if (ccg == null) {
                        ccg = new HashMap<>();
                        nhs.put(onsCode,ccg);
                    }

                    Date reportDate = hisFormat.parse(nextLine[1]);
                    NHSStat report = ccg.get(reportDate);
                    if (report == null) {
                        report = new NHSStat();
                        report.org = onsCode;
                        report.date = reportDate;

                        Location location = locations.get(onsCode);
                        if (location != null) {
                            ccg.put(reportDate,report);
                        } else {
                            if (missinglocation.get(onsCode) == null) {
                                missinglocation.put(onsCode,nextLine[5]);
                            }
                        }
                    }
                    if (header.length> 8 && header[8].equals("TriageCount")) {
                        switch (nextLine[2].trim().toLowerCase()) {
                            case "female":
                                report.femaleTriage += Integer.parseInt(nextLine[8]);
                                break;
                            case "male":
                                report.maleTriage += Integer.parseInt(nextLine[8]);
                                break;
                            default:
                                report.unknownTriage += Integer.parseInt(nextLine[8]);
                        }

                    } else if (header.length> 6 && header[6].equals("TriageCount")) {
                        switch (nextLine[2].trim().toLowerCase()) {
                            case "female":
                                report.femaleTriage += Integer.parseInt(nextLine[6]);
                                break;
                            case "male":
                                report.maleTriage += Integer.parseInt(nextLine[6]);
                                break;
                            default:
                                report.unknownTriage += Integer.parseInt(nextLine[6]);
                        }
                    }
                    else {
                        throw new InternalErrorException("Unable to process NHS Pathways data file");
                    }
                }

                count++;
            }


    }

    private void GetNHSOnlineData(String fileUrl) throws Exception {
        BufferedInputStream zis = new BufferedInputStream(new URL(fileUrl).openStream());
        try {
            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader, ',', '\"', 0));
            String[] header = null;
            int count = 0;
            for (CSVIterator it = iterator; it.hasNext(); ) {

                if (count == 0) {
                    header = it.next();
                } else {
                    String[] nextLine = it.next();
                    String onsCode = GetMergedId(nextLine[3]);
                    Map<Date, NHSStat> ccg = nhs.get(onsCode);
                    if (ccg == null) {
                        ccg = new HashMap<>();
                        nhs.put(onsCode,ccg);
                    }
                    Date reportDate = hisFormat.parse(nextLine[0]);
                    NHSStat report = ccg.get(reportDate);
                    if (report == null) {
                        report = new NHSStat();
                        report.org = onsCode;
                        report.date = reportDate;
                        Location location = locations.get(onsCode);
                        if (location != null) {
                            ccg.put(reportDate,report);
                        } else {
                            if (missinglocation.get(onsCode) == null) {
                                missinglocation.put(onsCode,nextLine[4]);
                            }
                        }
                    }
                    if (header.length>5 && header[5].equals("Total")) {
                        switch (nextLine[1].trim().toLowerCase()) {
                            case "female":
                                report.femaleOnline += Integer.parseInt(nextLine[5]);
                                break;
                            case "male":
                                report.maleOnline += Integer.parseInt(nextLine[5]);
                                break;
                            default:
                                report.unknownOnline += Integer.parseInt(nextLine[5]);
                        }
                    } else if (header.length>7 && header[7].equals("Total")) {
                        switch (nextLine[1].trim().toLowerCase()) {
                            case "female":
                                report.femaleOnline += Integer.parseInt(nextLine[7]);
                                break;
                            case "male":
                                report.maleOnline += Integer.parseInt(nextLine[7]);
                                break;
                            default:
                                report.unknownOnline += Integer.parseInt(nextLine[7]);
                        }
                    }
                    else {
                        throw new InternalErrorException("Unable to process NHS Online data file");
                    }
                }
                count++;
            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }

    }

    private void ProcessDeprivation() {
        InputStream zis = classLoader.getResourceAsStream("Deprivation.csv");
        try {
            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader, ',', '\"', 0));
            String[] header = null;
            int count = 0;
            for (CSVIterator it = iterator; it.hasNext(); ) {

                if (count == 0) {
                    header = it.next();
                } else {
                    String[] nextLine = it.next();
                    hi.put(nextLine[0],new BigDecimal(nextLine[1]));
                    mdi.put(nextLine[0],new BigDecimal(nextLine[2]));
                }
                count++;
            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    private void ProcessPHEExcelFile() throws Exception {

        reports = new ArrayList<>();

            BufferedInputStream zis = new BufferedInputStream(new URL(PHE_EXCEL).openStream());
            Workbook wb = new XSSFWorkbook(zis);
            Sheet  sheet = wb.getSheet("UTLAs");
            if (sheet != null) {
                Row header = sheet.getRow(7);
                for(int i =9;i < sheet.getLastRowNum(); i++ ) {
                    Row row = sheet.getRow(i);
                    if (row != null && row.getCell(0)!= null) {
                        String onsCode = row.getCell(0).getStringCellValue();
                        Date lastColumnDate = null;
                        for (int f = 3; f < row.getLastCellNum(); f++) {
                            Date columnDate = null;
                            try {
                                columnDate = header.getCell(f).getDateCellValue();
                                if (lastColumnDate == null || !lastColumnDate.toInstant().equals(columnDate.toInstant())) {
                                    MeasureReport report = getPHEMeasureReport(Date.from(columnDate.toInstant()),
                                            (int) row.getCell(f).getNumericCellValue(),
                                            onsCode);
                                    if (report != null) this.reports.add(report);
                                }
                                if (lastColumnDate != null && lastColumnDate.toInstant().equals(columnDate.toInstant())) {
                                    log.warn("Duplicate entry in PHE Historic file for {}", columnDate.toInstant());
                                }
                            } catch (Exception ex) {
                                log.info("OnsCode {} Row Number {} Cell NUmber {}", onsCode, i, f);
                                log.info("columnDate {}", columnDate);

                                throw ex;
                            }
                            lastColumnDate = columnDate;
                        }
                    }
                }

                CalculatePHERegions();

                UploadReports();

            }


    }


    private void  CalculatePHERegions() {
        Map<String , Map<Date, BigDecimal>> pheMap = new HashMap<>();
        for (MeasureReport report : this.reports) {
            String onsCode = report.getSubject().getIdentifier().getValue();
            Location location = this.locations.get(onsCode);
            if (location != null) {
                String parentONS = location.getPartOf().getIdentifier().getValue();
                if (parentONS != null) {

                    if (pheMap.get(parentONS) == null) {
                        pheMap.put(parentONS, new HashMap<>());
                    }
                    Map<Date,BigDecimal> parentMap = pheMap.get(parentONS);
                    if (parentMap.get(report.getDate()) ==null) {
                        parentMap.put(report.getDate(),new BigDecimal(0));
                    }
                    for (MeasureReport.MeasureReportGroupComponent group : report.getGroup()) {
                        if (group.getCode().getCodingFirstRep().getCode().equals("840539006")) {
                            parentMap.replace(report.getDate(),
                                    group.getMeasureScore().getValue().add(parentMap.get(report.getDate())));
                        }
                    }
                } else {
                    if (!onsCode.equals("E92000001")) throw new InternalError("Missing Parent Location "+ onsCode);
                }

            } else {
                throw new InternalError("Missing Location "+ onsCode);
            }

        }

        for (Map.Entry<String,Map<Date,BigDecimal>> parent : pheMap.entrySet()) {
            for(Map.Entry<Date,BigDecimal> dayEntry : parent.getValue().entrySet()) {
                MeasureReport report = getPHEMeasureReport(dayEntry.getKey(),
                        dayEntry.getValue().intValue(),
                        parent.getKey());
                this.reports.add(report);
            }
        }
    }

    private MeasureReport getPHEMeasureReport(Date reportDate, int cases, String onsCode) {
        MeasureReport report = new MeasureReport();

        Location location = locations.get(onsCode);


        if (location != null) {
            int population = ((IntegerType) location.getExtensionByUrl("https://fhir.mayfield-is.co.uk/Population").getValue()).getValue();
            report.addIdentifier()
                    .setSystem("https://www.arcgis.com/fhir/CountyUAs_cases")
                    .setValue(onsCode + "-" + stamp.format(reportDate));

            report.setDate(reportDate);
            report.setPeriod(new Period().setStart(reportDate));
            report.setStatus(MeasureReport.MeasureReportStatus.COMPLETE);
            report.setType(MeasureReport.MeasureReportType.SUMMARY);

            report.getReporter()
                    .setDisplay(location.getName())
                    .setReference(location.getId());
            report.getReporter().setIdentifier(new Identifier().setSystem(ONSSystem).setValue(onsCode));

            report.getSubject()
                    .setDisplay(location.getName())
                    .setReference(location.getId());
            report.getSubject().setIdentifier(new Identifier().setSystem(ONSSystem).setValue(onsCode));

            report.setMeasure(phe);
            Quantity qty = new Quantity();
            qty.setValue(new BigDecimal(cases));

            MeasureReport.MeasureReportGroupComponent group = report.addGroup();
            group.setCode(
                    new CodeableConcept().addCoding(
                            new Coding().setSystem("http://snomed.info/sct")
                                    .setCode("840539006")
                                    .setDisplay("Disease caused by severe acute respiratory syndrome coronavirus 2 (disorder)")
                    )
            )
                    .addPopulation().setCount(population);
            group.setMeasureScore(qty);

            group = report.addGroup();
            group.setCode(
                    new CodeableConcept().addCoding(
                            new Coding().setSystem("http://fhir.mayfield-is.co.uk")
                                    .setCode("CASES/MILLION")
                                    .setDisplay("COVID-19 Cases Per million")
                    )
            )
                    .addPopulation().setCount(1000000);
            Quantity qtyadj = new Quantity();
            Double num = (qty.getValue().doubleValue() / population) * 1000000;
            qtyadj.setValue(num);
            group.setMeasureScore(qtyadj);

            Extension hi = location.getExtensionByUrl("https://fhir.mayfield-is.co.uk/HI");
            if (hi != null) {
                group = report.addGroup();
                group.setCode(
                        new CodeableConcept().addCoding(
                                new Coding().setSystem("http://fhir.mayfield-is.co.uk")
                                        .setCode("HI")
                                        .setDisplay("Health Index")
                        )
                )
                        .addPopulation().setCount(32845);

                group.setMeasureScore((Quantity) hi.getValue());
            }
            Extension mdi = location.getExtensionByUrl("https://fhir.mayfield-is.co.uk/MDI");

            if (mdi != null) {
                group = report.addGroup();
                group.setCode(
                        new CodeableConcept().addCoding(
                                new Coding().setSystem("http://fhir.mayfield-is.co.uk")
                                        .setCode("MDI")
                                        .setDisplay("Multiple Depravity Index")
                        )
                )
                        .addPopulation().setCount(32845);

                group.setMeasureScore((Quantity) mdi.getValue());
            }

            Extension hect = location.getExtensionByUrl("https://fhir.mayfield-is.co.uk/AREAEHECT");

            if (hect != null) {
                group = report.addGroup();
                group.setCode(
                        new CodeableConcept().addCoding(
                                new Coding().setSystem("http://fhir.mayfield-is.co.uk")
                                        .setCode("PERHECT")
                                        .setDisplay("Cases Per Hectare")
                        )
                )
                        .addPopulation().setCount(((Quantity) hect.getValue()).getValue().intValue());

                Quantity perhect = new Quantity();
                Double numPerHect = (qty.getValue().doubleValue() / ((Quantity) hect.getValue()).getValue().doubleValue()) ;
                perhect.setValue(numPerHect);
                group.setMeasureScore(perhect);
            }


            return report;
        } else {
            log.error("Missing Location Id");
            return null;
        }
    }

    private MeasureReport getMorbidityMeasureReport(Date reportDate, int cases, String onsCode) {
        MeasureReport report = new MeasureReport();

        Location location = locations.get(onsCode);


        if (location != null) {
            int population = 0;
            Extension locExt = location.getExtensionByUrl("https://fhir.mayfield-is.co.uk/Population");
            if (locExt != null) {
                population = ((IntegerType) locExt.getValue()).getValue();
            }
            report.addIdentifier()
                    .setSystem("https://www.arcgis.com/fhir/Morbidity")
                    .setValue(onsCode + "-" + stamp.format(reportDate));

            report.setDate(reportDate);
            report.setPeriod(new Period().setStart(reportDate));
            report.setStatus(MeasureReport.MeasureReportStatus.COMPLETE);
            report.setType(MeasureReport.MeasureReportType.SUMMARY);

            report.getReporter()
                    .setDisplay(location.getName())
                    .setReference(location.getId());
            report.getReporter().setIdentifier(new Identifier().setSystem(ONSSystem).setValue(onsCode));

            report.getSubject()
                    .setDisplay(location.getName())
                    .setReference(location.getId());
            report.getSubject().setIdentifier(new Identifier().setSystem(ONSSystem).setValue(onsCode));

            report.setMeasure(morbidity);
            Quantity qty = new Quantity();
            qty.setValue(new BigDecimal(cases));

            MeasureReport.MeasureReportGroupComponent group = report.addGroup();
            group.setCode(
                    new CodeableConcept().addCoding(
                            new Coding().setSystem("http://snomed.info/sct")
                                    .setCode("255619001|419620001")
                                    .setDisplay("Total Deaths")
                    )
            )
                    .addPopulation().setCount(population);
            group.setMeasureScore(qty);



            return report;
        } else {
            log.error("Missing Location Id");
            return null;
        }
    }

    private void ProcessLocationsFile(String fileName, String type) throws Exception {

        InputStream zis = classLoader.getResourceAsStream(fileName);
        log.info("Processing Locations "+ type);
        LAHandler laHandler = new LAHandler(type);
        Process(zis, laHandler);

        Bundle bundle = null;

        int count = 0;
        int fileCnt=0;
        for (Location location : locations.values() ) {

            if ((count % batchSize) == 0 ) {

               if (bundle != null) processLocations(bundle, fileCnt);
                bundle = new Bundle();
                bundle.getIdentifier().setSystem("https://fhir.mayfield-is.co.uk/Id/")
                        .setValue(UUID.randomUUID().toString());

                bundle.setType(Bundle.BundleType.TRANSACTION);
                fileCnt++;
            }
            Bundle.BundleEntryComponent entry = bundle.addEntry()
                    .setFullUrl(UUID_Prefix + UUID.randomUUID().toString())
                    .setResource(location);
            String conditionalUrl = getConditional(location.getIdentifierFirstRep());
            entry.getRequest()
                    .setMethod(Bundle.HTTPVerb.PUT)
                    .setUrl("Location?" + conditionalUrl);
            count++;
        }
        if (bundle != null && bundle.getEntry().size() > 0) {
            processLocations(bundle,fileCnt);
        }



// Log the response

    }

    private void ProcessPopulationsFile(String fileName) throws Exception {

        InputStream zis = classLoader.getResourceAsStream(fileName);


            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader, ',', '\"', 0));
            int count = 0;
            for (CSVIterator it = iterator; it.hasNext(); ) {

                if (count == 0) {
                    it.next();
                } else {
                    String[] nextLine = it.next();
                    if (!nextLine[0].isEmpty()) {
                        String onsCode = GetMergedId(nextLine[0]);
                        try {

                            if (population.get(onsCode) != null) {
                                BigDecimal pop = population.get(onsCode);
                                pop = pop.add(new BigDecimal(nextLine[4]));
                                population.replace(onsCode, pop);
                            } else {
                                population.put(onsCode, new BigDecimal(nextLine[4]));
                            }
                        }
                        catch (Exception ex ) {
                            log.warn("{} invalid population count {}",onsCode,nextLine[4]);
                        }
                    }
                }
                count++;
            }

// Log the response

    }

    private void processLocations(Bundle bundle, int fileCount) throws Exception {
        //Path path = Paths.get("Locations-"+fileCount+".json");
        //Files.write(path,ctxFHIR.newJsonParser().setPrettyPrint(true).encodeResourceToString(bundle).getBytes() );



            Bundle resp = client.transaction().withBundle(bundle).execute();
            int index = 0;
            for (Bundle.BundleEntryComponent entry : resp.getEntry()) {
                if (entry.getResource() instanceof Bundle) {
                    if (entry.getResponse().getStatus().startsWith("200")) {
                    for (Bundle.BundleEntryComponent entrySub : ((Bundle) entry.getResource()).getEntry()) {
                        if (entrySub.getResource() instanceof Location) {
                            Location found = (Location) entrySub.getResource();
                            locations.replace(found.getIdentifierFirstRep().getValue(), found);
                        }
                    }
                }}
                else {
                    if (entry.hasResponse() && entry.getResponse().hasLocation()) {
                        Location original = (Location) bundle.getEntry().get(index).getResource();
                        Location location = locations.get(original.getIdentifierFirstRep().getValue());
                        location.setId(entry.getResponse().getLocation());
                    }
                }
                index++;
            }


    }





    private void sendMeasures(Bundle bundle, int fileCount) throws Exception {

        if (bundle.getEntryFirstRep() != null) {
            MeasureReport t = (MeasureReport) bundle.getEntryFirstRep().getResource();
       //     log.info("Processing {} Cases {}",  t.getMeasure(), t.getIdentifierFirstRep().getValue());
        }

        Bundle resp = client.transaction().withBundle(bundle).execute();

// Log the response
       log.debug(ctxFHIR.newJsonParser().setPrettyPrint(true).encodeResourceToString(resp));

    }

    private String getConditional(Identifier identifier) {

        if (identifier.hasSystem()) return "identifier="+identifier.getSystem()+"|"+identifier.getValue();
        return "identifier="+identifier.getValue();
    }

    private interface IRecordHandler {
        void accept(String[] theRecord);
    }

    private void Process(InputStream zis, IRecordHandler handler) throws Exception {

        char delimiter = ',';

            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader,',', '\"', 1));

            for (CSVIterator it = iterator; it.hasNext(); ) {
                String[] nextLine = it.next();
                handler.accept(nextLine);
            }


    }

    public class LAHandler implements IRecordHandler
    {


        LAHandler(String type) {
            this.type = type;
        }

        String type;

        @Override
        public void accept(String[] theRecord) {

           Location location = new Location();
            location.addIdentifier().setSystem(ONSSystem).setValue(theRecord[0]);
            location.setName(theRecord[1]);

            if (theRecord[7] != null && !theRecord[7].isEmpty()) {
                location.getPartOf()
                        .getIdentifier()
                        .setSystem(ONSSystem).setValue(theRecord[7]);
                Location parent = locations.get(theRecord[7]);
                if (parent != null) {
                    location.getPartOf()
                            .setReference(parent.getId());
                } else {
                    throw new InternalError("Missing parent code for "+ theRecord[0]+ " Entry " + theRecord[7]);
                }
            } else {
                log.warn("Parent code empty for {}", theRecord[0]);
            }


            BigDecimal pop = population.get(theRecord[0]);
            if (pop != null) {
                Extension extensionpop = location.addExtension();
                extensionpop.setUrl("https://fhir.mayfield-is.co.uk/Population");
                extensionpop.setValue(new IntegerType().setValue(pop.intValue()));
            }

            BigDecimal hibd = hi.get(theRecord[0]);
            if (hibd != null) {
                Extension extension = location.addExtension();
                extension.setUrl("https://fhir.mayfield-is.co.uk/HI");
                extension.setValue(new Quantity().setValue(hibd));
            }
            BigDecimal mdibd = mdi.get(theRecord[0]);
            if (mdibd != null) {
                Extension extension = location.addExtension();
                extension.setUrl("https://fhir.mayfield-is.co.uk/MDI");
                extension.setValue(new Quantity().setValue(mdibd));
            }
            // Area
            if (theRecord[11] != null && !theRecord[11].isEmpty()) {
                Extension extension = location.addExtension();
                extension.setUrl("https://fhir.mayfield-is.co.uk/AREAEHECT");
                extension.setValue(new Quantity().setValue(Float.parseFloat(theRecord[11])));
            }
            location.addType().addCoding().setSystem("https://fhir.mayfield-is.co.uk/TYPE").setCode(type);
            // Ignore old entries, records have duplicates

            if (locations.get(theRecord[0]) != null) {
                if (!theRecord[10].contains("terminated")) {
                    locations.replace(theRecord[0], location);
                }
            } else {
                // Record doesn't exist so add
                locations.put(theRecord[0], location);
            }

        }

    }

    private void ProcessPHEDailyUAFile() throws Exception {

        Date today = new Date();
        LocalDateTime ldt = LocalDateTime.ofInstant(today.toInstant(), ZoneId.systemDefault());
        ldt = ldt.minusDays(1);
        today = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());

        reports = new ArrayList<>();

        BufferedInputStream in = new BufferedInputStream(new URL(PHE_UACASES_URL).openStream());

        CaseHandler handler = new CaseHandler(today);
        Process(in, handler);

        CalculatePHERegions();

        UploadReports();

    }

    public class CaseHandler implements IRecordHandler
    {

        Date reportDate;
        CaseHandler(Date reportDate) {
            this.reportDate = reportDate;
        }


        @Override
        public void accept(String[] theRecord) {

            MeasureReport report = new MeasureReport();

            Location location = locations.get(theRecord[0]);

            if (location != null) {

                String onsCode = GetMergedId(theRecord[0]);

                String qty = theRecord[2].trim().replace(",","");

                report = getPHEMeasureReport(reportDate,
                        Integer.parseInt(qty),
                        onsCode);
                if (report != null) reports.add(report);

            } else {
                log.error("Missing Location " + theRecord[0]);
            }

        }

    }

}
