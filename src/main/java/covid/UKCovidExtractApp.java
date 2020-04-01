package covid;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.opencsv.CSVIterator;
import com.opencsv.CSVReader;
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


    final String UUID_Prefix = "urn:uuid:";

    final String ONSSystem = "https://fhir.gov.uk/Identifier/ONS";

    final int batchSize = 10;

    FhirContext ctxFHIR = FhirContext.forR4();

    private ArrayList<MeasureReport> reports = new ArrayList<>();

    private Map<String,Location> locations = new HashMap<>();

    private Map<String, BigDecimal> hi = new HashMap<>();
    private Map<String, BigDecimal> mdi = new HashMap<>();
    private Map<String, BigDecimal> population = new HashMap<>();

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

        ProcessDeprivation();

        // Population

        log.info("Processing Locations");
        ProcessPopulationsFile("UK.csv");
        ProcessPopulationsFile("EnglandRegions.csv");
        ProcessPopulationsFile("LocalAuthority.csv");

        // Locations
        ProcessLocationsFile("E12_RGN");
        /*

  //      Disable for now, can use to correct past results.
  //      ProcessHistoric();

        // Process Daily UA File
        reports = new ArrayList<>();
        ProcessDailyUAFile(today);
        CalculateRegions(dateStamp.format(today));
*/
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
               //     log.info("{} cnt = {} {}",nextLine[0],nextLine[1], nextLine[2]);
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
                } else  {
                    String[] nextLine = it.next();
                    Bundle bundle = new Bundle();
                    bundle.getIdentifier().setSystem("https://fhir.mayfield-is.co.uk/Id/")
                            .setValue(UUID.randomUUID().toString());
                    bundle.setType(Bundle.BundleType.TRANSACTION);
                    for (int i = 2; i < nextLine.length; i++) {
                        log.info("{} {} count {}", nextLine[0], header[i], nextLine[i]);
                        MeasureReport report = getMeasureReport(hisFormat.parse(header[i]) ,Integer.parseInt(nextLine[i].trim()),nextLine[0]);
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
                   // log.info(ctxFHIR.newJsonParser().setPrettyPrint(true).encodeResourceToString(bundle));
                    Bundle resp = client.transaction().withBundle(bundle).execute();
                }
                count++;
            }
            for (int i = 2; i < header.length; i++) {
                CalculateRegions(dateStamp.format(hisFormat.parse(header[i])));
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

                Bundle results = client.search().byUrl("MeasureReport?reporter.partof.identifier="+nextLine[0]+"&date=ge"+date+"T00:00&date=le"+date+"T23:59&_count=50").returnBundle(Bundle.class).execute();
                //log.info(ctxFHIR.newXmlParser().setPrettyPrint(true).encodeResourceToString(results));
                int cases =0;
                for(Bundle.BundleEntryComponent entryComponent : results.getEntry()) {
                    if (entryComponent.getResource() instanceof MeasureReport) {
                        cases +=  ((MeasureReport) entryComponent.getResource()).getGroupFirstRep().getMeasureScore().getValue().intValue();
                    }
                }
                log.info("{} Cases Count = {}",nextLine[0], cases);

                Date reportDate = null;
                try {
                    reportDate = dateStamp.parse(date);
                } catch (Exception ex) {
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
         //   log.info(ctxFHIR.newJsonParser().setPrettyPrint(true).encodeResourceToString(bundle));
            Bundle resp = client.transaction().withBundle(bundle).execute();
        } catch (IOException e) {
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
            report.setMeasure("https://www.arcgis.com/fhir/Measure/CountyUAs_cases");
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

            report.getSubject()
                    .setDisplay(location.getName())
                    .setReference(location.getId());
            return report;
        } else {
            log.error("Missing Location Id");
            return null;
        }
    }

    private void ProcessLocationsFile(String fileName) throws Exception {

        InputStream zis = classLoader.getResourceAsStream(fileName);

        LAHandler laHandler = new LAHandler();
        Process(zis, laHandler);

        Bundle bundle = null;

        int count = 0;
        int fileCnt=0;
        for (Location location : locations.values() ) {

            if ((count % batchSize) == 0 ) {

               // if (bundle != null) processLocations(bundle, fileCnt);
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
        // TODO    processLocations(bundle,fileCnt);
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
                    log.info("{} cnt = {}",nextLine[0],nextLine[4]);
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
        log.info("Processing Locations "+ fileCount);
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


        LAHandler() {

        }


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

            location.getPartOf()
                    .getIdentifier()
                    .setSystem(ONSSystem).setValue(theRecord[7]);

            BigDecimal pop = population.get(theRecord[0]);
            Extension extensionpop = location.addExtension();
            extensionpop.setUrl("https://fhir.mayfield-is.co.uk/Population");
            extensionpop.setValue(new IntegerType().setValue(pop.intValue()));

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
            locations.put(theRecord[0],location);
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
                report.setMeasure("https://www.arcgis.com/fhir/Measure/CountyUAs_cases");

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
