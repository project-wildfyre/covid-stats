package covid;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.opencsv.CSVIterator;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.apache.commons.io.Charsets;
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

        int maleTriage = 0;
        int femaleTriage = 0;
        int unknownTriage = 0;

        int maleOnline = 0;
        int femaleOnline = 0;
        int unknownOnline = 0;

        int population= 0;

    }

    String phe;
    String uec;

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
    private Map<String, Map<Date, NHSStat>> nhsParent;

    // private Map<String, Map<Date, NHSStat>> nhsStatMap = new HashMap<>();

    ClassLoader classLoader = getClass().getClassLoader();

    public static void main(String[] args) {
        SpringApplication.run(UKCovidExtractApp.class, args);
    }

    String PHE_URL = "https://www.arcgis.com/sharing/rest/content/items/b684319181f94875a6879bbc833ca3a6/data";
    String NHS_URL = "https://files.digital.nhs.uk/8E/AE4094/NHS%20Pathways%20Covid-19%20data%202020-04-02.csv";
    String NHSONLINE_URL = "https://files.digital.nhs.uk/9D/E01A56/111%20Online%20Covid-19%20data_2020-04-02.csv";

    DateFormat dateStamp = new SimpleDateFormat("yyyy-MM-dd");

    DateFormat hisFormat = new SimpleDateFormat("dd/MM/yyyy");

    DateFormat stamp = new SimpleDateFormat("yyyyMMdd");

    Date today = null;

    IGenericClient client = ctxFHIR.newRestfulGenericClient("https://fhir.test.xgenome.co.uk/R4");
    //IGenericClient client = ctxFHIR.newRestfulGenericClient("http://fhirserver-env-1.eba-aepmzc4d.eu-west-2.elasticbeanstalk.com:8186/R4");
   // IGenericClient client = ctxFHIR.newRestfulGenericClient("https://fhir-test-526344451.eu-west-2.elb.amazonaws.com/R4");

    @Override
    public void run(String... args) throws Exception {
        if (args.length > 0 && args[0].equals("exitcode")) {
            throw new Exception();
        }
        Date in = new Date();
        LocalDateTime ldt = LocalDateTime.ofInstant(in.toInstant(), ZoneId.systemDefault());
        // Set to date before
       // ldt = ldt.minusDays(1);
        today = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());

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


        ProcessDeprivation();
        /*
        for (int i = 1; i<10;i++) {
            RemoveOrgReport("E3900000"+i);
        }*/

        // Population

        log.info("Processing Population");
        ProcessPopulationsFile("UK.csv");
        ProcessPopulationsFile("EnglandRegions.csv");
        ProcessPopulationsFile("LocalAuthority.csv");
        ProcessPopulationsFile("2019-ccg-estimates.csv");

        // Locations
        log.info("Processing Locations");
        ProcessLocationsFile("E92_CTRY.csv","CTRY");
        ProcessLocationsFile("E12_RGN.csv","RGN");
        ProcessLocationsFile("E11_MCTY.csv","MCTY");
        ProcessLocationsFile("E10_CTY.csv","CTY");
        ProcessLocationsFile("E09_LONB.csv","LONB");
        ProcessLocationsFile("E08_MD.csv","MD");
        ProcessLocationsFile("E07_NMD.csv","NMD");
        ProcessLocationsFile("E06_UA.csv","UA");
     // Not required   ProcessLocationsFile("E05_WD.csv","WD");

        // These next three files have been modified to make sense
        ProcessLocationsFile("E40_ENG.csv","NHSENG");
        ProcessLocationsFile("E40_NHSER.csv","NHSER");
        // This is the main top level codes
        ProcessLocationsFile("E_OTHER.csv","NHSOTHER");
        ProcessLocationsFile("E39_NHSRLO.csv","NHSRLO");

        ProcessLocationsFile("E38_CCG.csv","CCG");
        ProcessLocationsFile("E_UNK.csv","NHS_OTHERREGION");

        ProcessLocationsFile("E12_RGN.csv","NHSRGN");
        ProcessLocationsFile("E06_UA.csv","NHSUA");

        reports = new ArrayList<>();
        PopulateNHS();


  //      Disable for now, can use to correct past results.
      //  log.info("Processing Past Data");
        reports = new ArrayList<>();
        ProcessHistoric();

        // Process Daily UA File
        reports = new ArrayList<>();
        ProcessDailyUAFile(today);

        log.info("Calculating Regions");
        CalculateRegions(dateStamp.format(today));

    }

    private void PopulateNHS() throws Exception {

        // https://digital.nhs.uk/data-and-information/publications/statistical/mi-potential-covid-19-symptoms-reported-through-nhs-pathways-and-111-online/latest
        GetNHSTriageData( NHS_URL );
        GetNHSOnlineData(NHSONLINE_URL);

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
            throw new InternalError("Missing data");
        }

        CalculateNHSRegional();

        Bundle bundle = null;

        int count = 0;
        int fileCnt=0;

        for (MeasureReport measureReport : this.reports) {

                if ((count % batchSize) == 0) {

                    if (bundle != null) processMeasures(bundle, fileCnt);
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
                processMeasures(bundle, fileCnt);
            }

    }


    private void ProcessMissingLocation() {
        File file = new File("MissingLocation.csv");
        try {
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
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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


                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","daily-triage","UEC Daily Total", nhs.femaleTriage+nhs.maleTriage+nhs.unknownTriage,null);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","daily-online","NHS 111 Website Daily Total", nhs.femaleOnline+nhs.maleOnline+nhs.unknownOnline,null);

                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male-online-total","UEC Male Total", maleOnlineTotal,null);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female-online-total","UEC Female Total", femaleOnlineTotal,null);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","online-total","COVID Symptoms Reported NHS 111 website", maleOnlineTotal + femaleOnlineTotal + unknownOnlineTotal,population);

                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male-triage-total","UEC Male Total", maleTriageTotal,null);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female-triage-total","UEC Female Total", femaleTriageTotal,null);
                addGroup(report,"http://snomed.info/sct","840544004","Suspected disease caused by severe acute respiratory coronavirus 2 (situation)", femaleTriageTotal + maleTriageTotal+ unknownTriageTotal,population);

                log.info("{} count {} {}", report.getIdentifierFirstRep().getValue(), nhs.femaleTriage + nhs.maleTriage, femaleTriageTotal + maleTriageTotal);

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
private void addGroup(MeasureReport report, String system, String code, String display, int qtyValue, BigDecimal population ) {
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
        Bundle bundle = client.search().byUrl("MeasureReport?reporter.identifier="+org).returnBundle(Bundle.class).execute();
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof MeasureReport) {
                client.delete().resourceById(((MeasureReport) entry.getResource()).getIdElement()).execute();
            }
        }

    }
    private void GetNHSTriageData(String fileUrl) throws Exception {
        BufferedInputStream zis = new BufferedInputStream(new URL(fileUrl).openStream());
        try {
            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader, ',', '\"', 0));
            String[] header = null;
            int count = 0;
            for (CSVIterator it = iterator; it.hasNext(); ) {

                if (count == 0) {
                    it.next();
                } else {
                    String[] nextLine = it.next();
                    Map<Date, NHSStat> ccg = nhs.get(nextLine[4]);
                    if (ccg == null) {
                        ccg = new HashMap<>();
                        nhs.put(nextLine[4],ccg);
                    }

                    Date reportDate = hisFormat.parse(nextLine[1]);
                    NHSStat report = ccg.get(reportDate);
                    if (report == null) {
                        report = new NHSStat();
                        report.org = nextLine[4];
                        report.date = reportDate;

                        Location location = locations.get(nextLine[4]);
                        if (location != null) {
                            ccg.put(reportDate,report);
                        } else {
                            if (missinglocation.get(nextLine[4]) == null) {
                                missinglocation.put(nextLine[4],nextLine[5]);
                            }
                        }
                    }
                    switch (nextLine[2].trim().toLowerCase()) {
                        case "female" :
                            report.femaleTriage += Integer.parseInt(nextLine[6]);
                            break;
                        case "male" :
                            report.maleTriage += Integer.parseInt(nextLine[6]);
                            break;
                        default:
                            report.unknownTriage += Integer.parseInt(nextLine[6]);
                    }
                }
                count++;
            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
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
                    Map<Date, NHSStat> ccg = nhs.get(nextLine[3]);
                    if (ccg == null) {
                        ccg = new HashMap<>();
                        nhs.put(nextLine[3],ccg);
                    }
                    Date reportDate = hisFormat.parse(nextLine[0]);
                    NHSStat report = ccg.get(reportDate);
                    if (report == null) {
                        report = new NHSStat();
                        report.org = nextLine[3];
                        report.date = reportDate;
                        Location location = locations.get(nextLine[3]);
                        if (location != null) {
                            ccg.put(reportDate,report);
                        } else {
                            if (missinglocation.get(nextLine[3]) == null) {
                                missinglocation.put(nextLine[3],nextLine[4]);
                            }
                        }
                    }

                    switch (nextLine[1].trim().toLowerCase()) {
                        case "female" :
                            report.femaleOnline += Integer.parseInt(nextLine[5]);
                            break;
                        case "male" :
                            report.maleOnline += Integer.parseInt(nextLine[5]);
                            break;
                        default:
                            report.unknownOnline += Integer.parseInt(nextLine[5]);
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

    private void ProcessHistoric() {
        InputStream zis = classLoader.getResourceAsStream("Historic.csv");

        try {
            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader, ',', '\"', 0));
            String[] header = null;
            int count = 0;
            for (CSVIterator it = iterator; it.hasNext(); ) {

                if (count==0) {
                    header=it.next();
                    log.debug("{} Historic", header[0]);
                } else  {
                    String[] nextLine = it.next();
                    Bundle bundle = new Bundle();
                    bundle.getIdentifier().setSystem("https://fhir.mayfield-is.co.uk/Id/")
                            .setValue(UUID.randomUUID().toString());
                    bundle.setType(Bundle.BundleType.TRANSACTION);
                    for (int i = 2; i < nextLine.length; i++) {
                        log.debug("{} {} Historic count {}", nextLine[0], header[i], nextLine[i]);
                        try {
                            MeasureReport report = getMeasureReport(hisFormat.parse(header[i]), Integer.parseInt(nextLine[i].replace(",","").trim()), nextLine[0]);
                            if (report != null) {

                                Bundle.BundleEntryComponent entry = bundle.addEntry()
                                        .setFullUrl(UUID_Prefix + UUID.randomUUID().toString())
                                        .setResource(report);
                                String conditionalUrl = getConditional(report.getIdentifierFirstRep());
                                entry.getRequest()
                                        .setMethod(Bundle.HTTPVerb.PUT)
                                        .setUrl(entry.getResource().getClass().getSimpleName() + "?" + conditionalUrl);
                            }
                        } catch (Exception ex) {
                            log.error("Not processed {} {} due to {}",nextLine[0],header[i], ex.getMessage());
                        }
                    }
                   // log.info(ctxFHIR.newJsonParser().setPrettyPrint(true).encodeResourceToString(bundle));
                    Bundle resp = client.transaction().withBundle(bundle).execute();
                }
                count++;
            }
            for (int i = 2; i < header.length; i++) {
                String date = header[i].replace("\"","");
                if (!date.isEmpty() ) {
                    CalculateRegions(dateStamp.format(hisFormat.parse(date)));
                }
            }


        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    private void  CalculateRegions(String date) {
        InputStream zis = classLoader.getResourceAsStream("EnglandRegions.csv");

        Bundle bundle = new Bundle();
        bundle.getIdentifier().setSystem("https://fhir.mayfield-is.co.uk/Id/")
                .setValue(UUID.randomUUID().toString());
        bundle.setType(Bundle.BundleType.TRANSACTION);


        try {
            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader,',', '\"', 1));

            for (CSVIterator it = iterator; it.hasNext(); ) {
                String[] nextLine = it.next();

                Date startDate = dateStamp.parse(date);
                LocalDateTime ldt = LocalDateTime.ofInstant(startDate.toInstant(), ZoneId.systemDefault());
                // Set to date before
                ldt = ldt.plusDays(1);
                Date endDate = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
                //dateStamp.format(endDate)

                String url = "MeasureReport?measure=21263&subject.partof.identifier="+
                        nextLine[0]+
                        "&date=ge"+date+
                        "&date=lt"+dateStamp.format(endDate)+
                        //+"T23:00:00.000+00:00"+
                        "&_count=50";
                log.info(url);
                Bundle results = client.search().byUrl(url).returnBundle(Bundle.class).execute();
                int cases =0;
                for(Bundle.BundleEntryComponent entryComponent : results.getEntry()) {
                    if (entryComponent.getResource() instanceof MeasureReport) {
                        cases +=  ((MeasureReport) entryComponent.getResource()).getGroupFirstRep().getMeasureScore().getValue().intValue();
                    }
                }
                log.info("{} Regional Cases Count = {}",nextLine[0], cases);

                Date reportDate = null;
                try {
                    reportDate = dateStamp.parse(date);
                } catch (Exception ex) {
                    log.error("Invalid date {}",date);
                    log.error(ex.getMessage());
                }
                MeasureReport report = getMeasureReport(reportDate,cases,nextLine[0]);

                if (report != null) {

                    Bundle.BundleEntryComponent entry = bundle.addEntry()
                            .setFullUrl(UUID_Prefix + UUID.randomUUID().toString())
                            .setResource(report);
                    String conditionalUrl = getConditional(report.getIdentifierFirstRep());
                    entry.getRequest()
                            .setMethod(Bundle.HTTPVerb.PUT)
                            .setUrl(entry.getResource().getClass().getSimpleName() + "?" + conditionalUrl);

                }


            }
           Bundle resp = client.transaction().withBundle(bundle).execute();
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
        catch (Exception e) {
            throw new InternalErrorException(e);
        }
    }

    private MeasureReport getMeasureReport(Date reportDate, int cases, String locationId) {
        MeasureReport report = new MeasureReport();

        Location location = locations.get(locationId);


        if (location != null) {
            int population = ((IntegerType) location.getExtensionByUrl("https://fhir.mayfield-is.co.uk/Population").getValue()).getValue();
            report.addIdentifier()
                    .setSystem("https://www.arcgis.com/fhir/CountyUAs_cases")
                    .setValue(locationId + "-" + stamp.format(reportDate));

            report.setDate(reportDate);
            report.setPeriod(new Period().setStart(reportDate));
            report.setStatus(MeasureReport.MeasureReportStatus.COMPLETE);
            report.setType(MeasureReport.MeasureReportType.SUMMARY);
            report.setReporter(new Reference().setDisplay(location.getName()).setReference(location.getId()));
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

            report.getSubject()
                    .setDisplay(location.getName())
                    .setReference(location.getId());
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

        try {
            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader, ',', '\"', 0));
            int count = 0;
            for (CSVIterator it = iterator; it.hasNext(); ) {

                if (count == 0) {
                    it.next();
                } else {
                    String[] nextLine = it.next();
                    if (!nextLine[0].isEmpty()) {
                        try {
                        population.put(nextLine[0], new BigDecimal(nextLine[4])); }
                        catch (Exception ex ) {
                            log.warn("{} invalid population count {}",nextLine[0],nextLine[4]);
                        }
                    }
                }
                count++;
            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
// Log the response

    }

    private void processLocations(Bundle bundle, int fileCount) throws Exception {
        //Path path = Paths.get("Locations-"+fileCount+".json");
        //Files.write(path,ctxFHIR.newJsonParser().setPrettyPrint(true).encodeResourceToString(bundle).getBytes() );

        try {

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

        } catch (Exception ex ) {
            log.error(ex.getMessage());
        }
    }

    private void ProcessDailyUAFile(Date reportDate) throws Exception {
        BufferedInputStream in = new BufferedInputStream(new URL(PHE_URL).openStream());

        CaseHandler handler = new CaseHandler(reportDate);
        Process(in, handler);

        Bundle bundle = null;

        int count = 0;
        int fileCnt=0;

        for (Iterator<MeasureReport> iterator = reports.iterator(); iterator.hasNext(); ) {
            MeasureReport measureReport = iterator.next();
            if ((count % batchSize) == 0 ) {

                if (bundle != null) processMeasures(bundle, fileCnt);
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
            processMeasures(bundle,fileCnt);
        }

    }

    private void ProcessDailyUAFileTemp() throws Exception {

        InputStream in = classLoader.getResourceAsStream("CountyUAs_cases_table.csv");
        Date indate = new Date();
        LocalDateTime ldt = LocalDateTime.ofInstant(indate.toInstant(), ZoneId.systemDefault());
        // Set to date before
        ldt = ldt.minusDays(2);
        Date reportDate = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
        CaseHandler handler = new CaseHandler(reportDate);
        Process(in, handler);

        Bundle bundle = null;

        int count = 0;
        int fileCnt=0;

        for (Iterator<MeasureReport> iterator = reports.iterator(); iterator.hasNext(); ) {
            MeasureReport measureReport = iterator.next();
            if ((count % batchSize) == 0 ) {

                if (bundle != null) processMeasures(bundle, fileCnt);
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
            processMeasures(bundle,fileCnt);
        }

    }


    private void processMeasures(Bundle bundle, int fileCount) throws Exception {
        log.info("Processing Cases "+ fileCount);

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

    private void Process(InputStream zis, IRecordHandler handler) {

        char delimiter = ',';


        try {
            Reader reader = new InputStreamReader(zis, Charsets.UTF_8);

            CSVIterator iterator = new CSVIterator(new CSVReader(reader,',', '\"', 1));

            for (CSVIterator it = iterator; it.hasNext(); ) {
                String[] nextLine = it.next();
                handler.accept(nextLine);
            }

        } catch (IOException e) {
            throw new InternalErrorException(e);
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
                int population = ((IntegerType) location.getExtensionByUrl("https://fhir.mayfield-is.co.uk/Population").getValue()).getValue();
                report.addIdentifier()
                        .setSystem("https://www.arcgis.com/fhir/CountyUAs_cases")
                        .setValue(theRecord[0] + "-" + stamp.format(reportDate));

                report.setDate(reportDate);
                report.setPeriod(new Period().setStart(reportDate));
                report.setStatus(MeasureReport.MeasureReportStatus.COMPLETE);
                report.setType(MeasureReport.MeasureReportType.SUMMARY);
                report.setReporter(new Reference().setDisplay(location.getName()).setReference(location.getId()));
                report.setMeasure(phe);

                report.getSubject()
                        .setDisplay(theRecord[1])
                        .setReference(location.getId());

                Quantity qty = new Quantity();
                qty.setValue(new BigDecimal(Integer.parseInt(theRecord[2].trim())));

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

                reports.add(report);
            } else {
                log.error("Missing Location " + theRecord[0]);
            }

        }

    }


}
