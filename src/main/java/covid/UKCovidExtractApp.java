package covid;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.opencsv.CSVIterator;
import com.opencsv.CSVReader;
import org.apache.commons.io.Charsets;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;


@SpringBootApplication
public class UKCovidExtractApp implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(UKCovidExtractApp.class);

    private class NHSStat {
        int nhs111 = 0;
        int nhs999 = 0;
        int nhsOnline = 0;
        int male = 0;
        int female = 0;
        int maleOnline = 0;
        int femaleOnline = 0;
        String parent;
    }

    String phe;
    String uec;

    final String UUID_Prefix = "urn:uuid:";

    final String ONSSystem = "https://fhir.gov.uk/Identifier/ONS";

    final int batchSize = 10;

    FhirContext ctxFHIR = FhirContext.forR4();

    private ArrayList<MeasureReport> reports = new ArrayList<>();

    private Map<String,Location> locations = new HashMap<>();

    private Map<String, BigDecimal> hi = new HashMap<>();
    private Map<String, BigDecimal> mdi = new HashMap<>();
    private Map<String, BigDecimal> population = new HashMap<>();

    private Map<String, Map<Date, MeasureReport>> nhs = new HashMap<>();

    private Map<String, Map<Date, NHSStat>> nhsStatMap = new HashMap<>();


  //  private Map<String,MeasureReport> pastMeasures = new HashMap<>();

    ClassLoader classLoader = getClass().getClassLoader();

    public static void main(String[] args) {
        SpringApplication.run(UKCovidExtractApp.class, args);
    }

    String FILE_URL = "https://www.arcgis.com/sharing/rest/content/items/b684319181f94875a6879bbc833ca3a6/data";

    DateFormat dateStamp = new SimpleDateFormat("yyyy-MM-dd");

    DateFormat hisFormat = new SimpleDateFormat("dd/MM/yyyy");

    DateFormat stamp = new SimpleDateFormat("yyyyMMdd");

    Date today = null;

  // IGenericClient client = ctxFHIR.newRestfulGenericClient("https://fhir.test.xgenome.co.uk/R4");
    IGenericClient client = ctxFHIR.newRestfulGenericClient("http://fhirserver-env-1.eba-aepmzc4d.eu-west-2.elasticbeanstalk.com:8186/R4");
   // IGenericClient client = ctxFHIR.newRestfulGenericClient("https://fhir-test-526344451.eu-west-2.elb.amazonaws.com/R4");

    @Override
    public void run(String... args) throws Exception {
        if (args.length > 0 && args[0].equals("exitcode")) {
            throw new Exception();
        }
        Date in = new Date();
        LocalDateTime ldt = LocalDateTime.ofInstant(in.toInstant(), ZoneId.systemDefault());
        // Set to date before
        ldt = ldt.minusDays(1);
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


        // Population

        log.info("Processing Locations");
        ProcessPopulationsFile("UK.csv");
        ProcessPopulationsFile("EnglandRegions.csv");
        ProcessPopulationsFile("LocalAuthority.csv");

        // Locations
        ProcessLocationsFile("E92_CTRY.csv","CTRY");
        ProcessLocationsFile("E12_RGN.csv","RGN");
        ProcessLocationsFile("E11_MCTY.csv","MCTY");
        ProcessLocationsFile("E10_CTY.csv","CTY");
        ProcessLocationsFile("E09_LONB.csv","LONB");
        ProcessLocationsFile("E08_MD.csv","MD");
        ProcessLocationsFile("E07_NMD.csv","NMD");
        ProcessLocationsFile("E06_UA.csv","UA");
     // Not required   ProcessLocationsFile("E05_WD.csv","WD");


        ProcessLocationsFile("E40_NHSER.csv","NHSER");
        ProcessLocationsFile("E39_NHSRLO.csv","NHSRLO");
        ProcessLocationsFile("E38_CCG.csv","CCG");
        ProcessLocationsFile("E12_RGN.csv","NHSRGN");
        ProcessLocationsFile("E06_UA.csv","NHSUA");
        PopulateNHS();


  //      Disable for now, can use to correct past results.
        log.info("Processing Past Data");
     //   ProcessHistoric();

        // Process Daily UA File
        reports = new ArrayList<>();
        ProcessDailyUAFile(today);

        log.info("Calculating Regions");
        CalculateRegions(dateStamp.format(today));

    }

    private void PopulateNHS() throws Exception {

        // https://digital.nhs.uk/data-and-information/publications/statistical/mi-potential-covid-19-symptoms-reported-through-nhs-pathways-and-111-online/latest
        GetNHSData("https://files.digital.nhs.uk/AE/AB9ABB/NHS%20Pathways%20Covid-19%20data%202020-03-31.csv");
        GetNHSOnlineData("https://files.digital.nhs.uk/EA/075299/111%20Online%20Covid-19%20data_2020-03-31.csv");

        for (Map.Entry<String, Map<Date, MeasureReport>> en : nhs.entrySet()) {
           // System.out.println("Key = " + en.getKey());
            processNHSReport(en);
        }

        CalculateNHSRegional();

        Bundle bundle = null;

        int count = 0;
        int fileCnt=0;

        for (Map.Entry<String, Map<Date, MeasureReport>> en : nhs.entrySet()) {
            for (Map.Entry<Date, MeasureReport> repMap : en.getValue().entrySet()) {
                MeasureReport measureReport = repMap.getValue();
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

    }

    private void CalculateNHSRegional() {
        for (Map.Entry<String, Map<Date, NHSStat>> org : nhsStatMap.entrySet()) {
            int maleTot =0;
            int femaleTot =0;
            int maleTotOnline =0;
            int femaleTotOnline =0;

            Map<Date, MeasureReport> ccg = nhs.get(org.getKey());
            if (ccg == null) {
                ccg = new HashMap<>();
                nhs.put(org.getKey(),ccg);
            }
            Map<Date, NHSStat> treeMap = new TreeMap(org.getValue());
            for (Map.Entry<Date, NHSStat> dateentry : treeMap.entrySet()) {
                // System.out.println("Key = " + dateentry.getKey());
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
                report.setMeasure(uec);

                Location location = locations.get(org.getKey());
                if (location != null) {
                    report.getSubject().setReference(location.getId());
                    report.getSubject().setDisplay(location.getName());
                    report.getReporter().setReference(location.getId());
                    report.getReporter().setDisplay(location.getName());
                }

                maleTot += nhs.male;
                femaleTot += nhs.female;
                femaleTotOnline += nhs.femaleOnline;
                maleTotOnline += nhs.maleOnline;

                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","nhs-online","NHS 111 Online", nhs.nhsOnline);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","nhs-111","NHS 111 Daily", nhs.nhs111);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","nhs-999","NHS 999 Daily", nhs.nhs999);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male","UEC Male Daily", nhs.male);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female","UEC Female Daily", nhs.female);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","daily-total","UEC Daily Total", nhs.female+nhs.male);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","daily-total-online","NHS 111 Website Daily Total", nhs.femaleOnline+nhs.maleOnline);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male-total","UEC Male Total", maleTot);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female-total","UEC Female Total", femaleTot);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male-total-online","UEC Male Total", maleTotOnline);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female-total-online","UEC Female Total", femaleTotOnline);
                addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","online-total","COVID Symptoms Reported NHS 111 website", maleTotOnline + femaleTotOnline);
                addGroup(report,"http://snomed.info/sct","840544004","Suspected disease caused by severe acute respiratory coronavirus 2 (situation)", femaleTot + maleTot);

                log.info("{} count {} {}", report.getIdentifierFirstRep().getValue(), nhs.female + nhs.male, femaleTot + maleTot);

                ccg.put(dateentry.getKey(),report);

            }
        }
    }



private void processNHSReport(Map.Entry<String, Map<Date, MeasureReport>> en) {
    Map<Date,MeasureReport> treeMap = new TreeMap(en.getValue());

    int maleTot = 0;
    int femaleTot = 0;
    int maleTotOnline = 0;
    int femaleTotOnline = 0;
    for ( Map.Entry<Date, MeasureReport> dateentry : treeMap.entrySet()) {
        // System.out.println("Key = " + dateentry.getKey());

        MeasureReport report = dateentry.getValue();
        NHSStat nhs = new NHSStat();

        for (MeasureReport.MeasureReportGroupComponent group : report.getGroup()) {
            if (group.getCode().getCodingFirstRep().getCode().startsWith("nhs111online")) {
                nhs.nhsOnline += group.getMeasureScore().getValue().intValue();
                if (group.getCode().getCodingFirstRep().getCode().contains("Female")) {
                    nhs.femaleOnline += group.getMeasureScore().getValue().intValue();
                } else {
                    nhs.maleOnline += group.getMeasureScore().getValue().intValue();
                }
            }
            if (group.getCode().getCodingFirstRep().getCode().startsWith("111")) {
                nhs.nhs111 += group.getMeasureScore().getValue().intValue();
                if (group.getCode().getCodingFirstRep().getCode().contains("Female")) {
                    nhs.female += group.getMeasureScore().getValue().intValue();
                } else {
                    nhs.male += group.getMeasureScore().getValue().intValue();
                }
            }
            if (group.getCode().getCodingFirstRep().getCode().startsWith("999")) {
                nhs.nhs999 += group.getMeasureScore().getValue().intValue();
                if (group.getCode().getCodingFirstRep().getCode().contains("Female")) {
                    nhs.female += group.getMeasureScore().getValue().intValue();
                } else {
                    nhs.male += group.getMeasureScore().getValue().intValue();
                }
            }
        }
        maleTot += nhs.male;
        femaleTot += nhs.female;
        maleTotOnline += nhs.maleOnline;
        femaleTotOnline += nhs.femaleOnline;

        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","nhs-online","NHS 111 Online", nhs.nhsOnline);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","nhs-111","NHS 111 Daily", nhs.nhs111);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","nhs-999","NHS 999 Daily", nhs.nhs999);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male","UEC Male Daily", nhs.male);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female","UEC Female Daily", nhs.female);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","daily-total","UEC Daily Total", nhs.female+nhs.male);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","daily-total-online","NHS 111 Website Daily Total", nhs.femaleOnline+nhs.maleOnline);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male-total","UEC Male Total", maleTot);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female-total","UEC Female Total", femaleTot);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","male-total-online","UEC Male Total", maleTotOnline);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","female-total-online","UEC Female Total", femaleTotOnline);
        addGroup(report,"http://fhir.mayfield-is.co.uk/CodeSystem/NHS-UEC-COVID","online-total","COVID Symptoms Reported NHS 111 website", maleTotOnline + femaleTotOnline);
        addGroup(report,"http://snomed.info/sct","840544004","Suspected disease caused by severe acute respiratory coronavirus 2 (situation)", femaleTot + maleTot);

        log.info("{} count {} {}", report.getIdentifierFirstRep().getValue(), nhs.female + nhs.male, femaleTot + maleTot);

        populateParent(report.getReporter().getIdentifier().getValue(),dateentry.getKey(),nhs);
    }
}


   private void  populateParent(String org,Date date,NHSStat nhs) {
        Location location = locations.get(org);
        if (location != null) {
            Location parent = locations.get(location.getPartOf().getIdentifier().getValue());
            if (parent != null) {

                Map<Date, NHSStat> nhsStat = nhsStatMap.get(parent.getIdentifierFirstRep().getValue());
                if (nhsStat == null) {
                    nhsStat = new HashMap<>();
                    nhsStatMap.put(parent.getIdentifierFirstRep().getValue(), nhsStat);
                }
                NHSStat parentStat = nhsStat.get(date);
                if (parentStat == null) {
                    parentStat = new NHSStat();
                    nhsStat.put(date, parentStat);
                }
                parentStat.female += nhs.female;
                parentStat.male += nhs.male;
                parentStat.maleOnline += nhs.maleOnline;
                parentStat.femaleOnline += nhs.femaleOnline;
                parentStat.nhs111 += nhs.nhs111;
                parentStat.nhs999 += nhs.nhs999;
                parentStat.nhsOnline += nhs.nhsOnline;
                populateParent(parent.getIdentifierFirstRep().getValue(), date, parentStat);
            }
        }
    }
private void addGroup(MeasureReport report, String system, String code, String display, int qtyValue ) {
    MeasureReport.MeasureReportGroupComponent group = report.addGroup();
    Quantity qty = new Quantity();
    BigDecimal value= new BigDecimal(qtyValue);
    qty.setValue(value);
    group.setMeasureScore(qty);

    group.setCode(new CodeableConcept().addCoding(
            new Coding().setSystem(system)
                    .setCode(code)
            .setDisplay(display)
    ));
//"http://fhir.mayfield-is.co.uk"

}

    private void GetNHSData(String fileUrl) throws Exception {
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
                    Map<Date, MeasureReport> ccg = nhs.get(nextLine[4]);
                    if (ccg == null) {
                        ccg = new HashMap<>();
                        nhs.put(nextLine[4],ccg);
                    }

                    Date reportDate = hisFormat.parse(nextLine[1]);
                    MeasureReport report = ccg.get(reportDate);
                    if (report == null) {
                        report = new MeasureReport();
                        report.addIdentifier()
                                .setSystem("https://fhir.mayfield-is.co.uk/Measure/NHS111")
                                .setValue(nextLine[4] + "-" + stamp.format(reportDate));

                        report.setDate(reportDate);
                        report.setPeriod(new Period().setStart(reportDate));
                        report.setStatus(MeasureReport.MeasureReportStatus.COMPLETE);
                        report.setType(MeasureReport.MeasureReportType.SUMMARY);
                        report.setReporter(new Reference().setDisplay(nextLine[5]).setIdentifier(new Identifier().setSystem(ONSSystem).setValue(nextLine[4])));
                        report.setMeasure(uec);

                        Location location = locations.get(nextLine[4]);
                        if (location != null) {
                            report.getSubject().setReference(location.getId());
                            report.getReporter().setReference(location.getId());
                            ccg.put(reportDate,report);
                        }
                    }
                    MeasureReport.MeasureReportGroupComponent group = report.addGroup();
                    Quantity qty = new Quantity();
                    BigDecimal value= new BigDecimal(Integer.parseInt(nextLine[6]));
                    qty.setValue(value);
                    group.setMeasureScore(qty);
                    String code = nextLine[3].trim().replace(" ", "");
                    if (code.isEmpty()) {
                        code="unknown";
                    }
                    code = nextLine[0] + "-" + nextLine[2].trim().replace(" ", "")+ "-"+ code;
                    group.setCode(new CodeableConcept().addCoding(
                            new Coding().setSystem("http://fhir.mayfield-is.co.uk")
                                    .setCode(code)

                    ));

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
                    Map<Date, MeasureReport> ccg = nhs.get(nextLine[3]);
                    if (ccg == null) {
                        ccg = new HashMap<>();
                        nhs.put(nextLine[3],ccg);
                    }
                    Date reportDate = hisFormat.parse(nextLine[0]);
                    MeasureReport report = ccg.get(reportDate);
                    if (report == null) {
                        report = new MeasureReport();

                        report.addIdentifier()
                                .setSystem("https://fhir.mayfield-is.co.uk/Measure/NHS111")
                                .setValue(nextLine[3] + "-" + stamp.format(reportDate));

                        report.setDate(reportDate);
                        report.setPeriod(new Period().setStart(reportDate));
                        report.setStatus(MeasureReport.MeasureReportStatus.COMPLETE);
                        report.setType(MeasureReport.MeasureReportType.SUMMARY);
                        report.setReporter(new Reference().setDisplay(nextLine[4]).setIdentifier(new Identifier().setSystem(ONSSystem).setValue(nextLine[3])));
                        report.setMeasure(uec);

                        Location location = locations.get(nextLine[3]);
                        if (location != null) {
                            report.getSubject().setReference(location.getId());
                            report.getReporter().setReference(location.getId());
                            ccg.put(reportDate,report);
                        }
                    }
                    MeasureReport.MeasureReportGroupComponent group = report.addGroup();
                    Quantity qty = new Quantity();
                    BigDecimal value= new BigDecimal(Integer.parseInt(nextLine[5]));
                    qty.setValue(value);
                    group.setMeasureScore(qty);
                    String code = nextLine[2].trim().replace(" ", "");
                    if (code.isEmpty()) {
                        code="unknown";
                    }
                    code = "nhs111online" + "-" + nextLine[1].trim().replace(" ", "")+ "-"+ code;
                    group.setCode(new CodeableConcept().addCoding(
                            new Coding().setSystem("http://fhir.mayfield-is.co.uk")
                                    .setCode(code)

                    ));

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
                String url = "MeasureReport?identifier=https://www.arcgis.com/fhir/CountyUAs_cases|&reporter.partof.identifier="+nextLine[0]+"&date=ge"+date+"&date=lt"+dateStamp.format(endDate)+"&_count=50";
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
                    population.put(nextLine[0],new BigDecimal(nextLine[4]));
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
        BufferedInputStream in = new BufferedInputStream(new URL(FILE_URL).openStream());

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
            Location parent = locations.get(theRecord[7]);
            if (parent != null) {
                location.getPartOf()
                        .setReference(parent.getId());
            }
            if (theRecord[7] != null && !theRecord[7].isEmpty()) {
                location.getPartOf()
                        .getIdentifier()
                        .setSystem(ONSSystem).setValue(theRecord[7]);
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
            if (!theRecord[10].contains("terminated")) {
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


       /*
    private void LoadYesterdays()  {
        Bundle bundle = null;
        Date indate = new Date();
        LocalDateTime ldt = LocalDateTime.ofInstant(indate.toInstant(), ZoneId.systemDefault());
        // Set to date before
        ldt = ldt.minusDays(2);
        Date reportDate = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());

        int count = 0;
        int fileCnt=0;
        for (Location location : locations.values() ) {

            if ((count % batchSize) == 0 ) {

                if (bundle != null) ProcessYesterday(bundle);
                bundle = new Bundle();
                bundle.getIdentifier().setSystem("https://fhir.mayfield-is.co.uk/Id/")
                        .setValue(UUID.randomUUID().toString());

                bundle.setType(Bundle.BundleType.TRANSACTION);
                fileCnt++;
            }
            Bundle.BundleEntryComponent entry = bundle.addEntry()
                    .setFullUrl(UUID_Prefix + UUID.randomUUID().toString());
            String conditionalUrl = "identifier=https://www.arcgis.com/fhir/CountyUAs_cases|" +  location.getIdentifierFirstRep().getValue() + "-" +stamp.format(reportDate);
            entry.getRequest()
                    .setMethod(Bundle.HTTPVerb.GET)
                    .setUrl("MeasureReport?" + conditionalUrl);
            count++;
        }
        if (bundle != null && bundle.getEntry().size() > 0) {
            ProcessYesterday(bundle);
        }


    }


    private void ProcessYesterday(Bundle bundle) {
        log.info("Processing Yesterdays data");
        Bundle resp = client.transaction().withBundle(bundle).execute();
        for (Bundle.BundleEntryComponent entry : resp.getEntry()) {
            if (entry.hasResource()) {
                for (Bundle.BundleEntryComponent result : ((Bundle) entry.getResource()).getEntry()) {
                    MeasureReport report = (MeasureReport) result.getResource();
                    pastMeasures.put(report.getReporter().getReference(),report);
                }
            }
        }
    }

     */

      /*
    private void FixLocation() {


        for (Location location : locations.values() ) {

            Bundle bundle = new Bundle();
            bundle.getIdentifier().setSystem("https://fhir.mayfield-is.co.uk/Id/")
                    .setValue(UUID.randomUUID().toString());

            bundle.setType(Bundle.BundleType.TRANSACTION);
            Bundle.BundleEntryComponent entry = bundle.addEntry()
                    .setFullUrl(UUID_Prefix + UUID.randomUUID().toString());
            String conditionalUrl = getConditional(location.getIdentifierFirstRep());
            entry.getRequest()
                    .setMethod(Bundle.HTTPVerb.GET)
                    .setUrl("Location?" + conditionalUrl);

            Bundle resp = client.transaction().withBundle(bundle).execute();

            for (Bundle.BundleEntryComponent delt : resp.getEntry()) {
                if (delt.getResource() instanceof Bundle && ((Bundle) delt.getResource()).getEntry().size()>1) {
                    Bundle delbundle = new Bundle();
                    delbundle.getIdentifier().setSystem("https://fhir.mayfield-is.co.uk/Id/")
                            .setValue(bundle.getId());

                    delbundle.setType(Bundle.BundleType.TRANSACTION);
                    //log.info(ctxFHIR.newJsonParser().setPrettyPrint(true).encodeResourceToString(resp));

                    for (Bundle.BundleEntryComponent del : ((Bundle) delt.getResource()).getEntry()) {
                        Bundle.BundleEntryComponent delentry = delbundle.addEntry()
                                .setFullUrl(UUID_Prefix + UUID.randomUUID().toString());

                        delentry.getRequest()
                                .setMethod(Bundle.HTTPVerb.DELETE)
                                .setUrl(del.getResource().getId());
                    }
                    log.info(ctxFHIR.newJsonParser().setPrettyPrint(true).encodeResourceToString(delbundle));
                    Bundle respdel = client.transaction().withBundle(delbundle).execute();
                }
            }
        }
    }

     */




}
