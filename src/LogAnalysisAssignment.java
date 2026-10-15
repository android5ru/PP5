import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
  ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
  - каждая строка полного запуска обработана ровно один раз;
  - для каждого URL requests = successful + errors;
  - maxDuration <= totalDuration при наличии запросов;
  - 0 <= activeFiles <= semaphoreLimit;
  - после завершения анализа activeFiles равно нулю;
  - один повреждённый файл не мешает обработке остальных;
  - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */
public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}

    public static LogEntry parseLine(String line){
        String[] parse = line.trim().split("\\s+");
        if (parse.length != 5){
            throw new IllegalArgumentException("Неверный формат строки: " + line);
        }
        return new LogEntry(parse[0], parse[1], Integer.parseInt(parse[2]), Long.parseLong(parse[3]), Long.parseLong(parse[4]));
    }
    /**
     * Возвращает отдельный итератор для чтения строк одного файла.
     *
     * ВАЖНО: Iterator<String> потоконебезопасен. Один экземпляр нельзя
     * одновременно читать из нескольких потоков. Для каждого потока нужен
     * отдельный вызов LogFileLines.iterator(path).
     */
    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    private void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    /** Генерирует реальные текстовые файлы с детерминированными данными. */
    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }

    /** Проверяет генератор и чтение строк без участия студенческого анализатора. */
    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Unexpected line: " + line
                        );
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }
    private static volatile boolean stopFlag = false;
    private static class UrlAccumulator {
        private long requests = 0;
        private long successful = 0;
        private long errors = 0;
        private long responseBytes = 0;
        private long totalDurationMillis = 0;
        private long maxDurationMillis = 0;

        public synchronized void add(LogEntry entry) {
            requests++;
            if (entry.status() >= 200 && entry.status() < 400) {
                successful++;
            } else {
                errors++;
            }
            responseBytes += entry.responseBytes();
            totalDurationMillis += entry.durationMillis();
            maxDurationMillis = Math.max(maxDurationMillis, entry.durationMillis());
        }

        public synchronized PathStatistics toStatistics() {
            return new PathStatistics(
                    requests, successful, errors,
                    responseBytes, totalDurationMillis, maxDurationMillis
            );
        }
    }

    private static class GlobalAccumulator {
        private final Map<String, UrlAccumulator> urlStats = new ConcurrentHashMap<>();

        public void add(String url, LogEntry entry) {
            UrlAccumulator acc = urlStats.computeIfAbsent(url, k -> new UrlAccumulator());
            acc.add(entry);
        }

        public synchronized Report buildReport() {
            long totalRequests = 0;
            long successfulRequests = 0;
            long errorRequests = 0;
            long responseBytes = 0;
            long maxDurationMillis = 0;

            for (UrlAccumulator acc : urlStats.values()) {
                PathStatistics stats = acc.toStatistics();
                totalRequests += stats.requests();
                successfulRequests += stats.successful();
                errorRequests += stats.errors();
                responseBytes += stats.responseBytes();
                maxDurationMillis = Math.max(maxDurationMillis, stats.maxDurationMillis());
            }

            return new Report(
                    totalRequests, successfulRequests, errorRequests,
                    responseBytes, maxDurationMillis
            );
        }
    }

    private static class GlobalCounters {
        public final AtomicLong processedFiles = new AtomicLong(0);
        public final AtomicLong readLines = new AtomicLong(0);
        public final AtomicLong correctEntries = new AtomicLong(0);
        public final AtomicLong corruptedEntries = new AtomicLong(0);
        public final AtomicLong readErrors = new AtomicLong(0);
        public final AtomicInteger activeFiles = new AtomicInteger(0);
        public final AtomicInteger maxActiveFiles = new AtomicInteger(0);
        public final List<String> failedFiles = Collections.synchronizedList(new ArrayList<>());

        public void updateMaxActive() {
            int current = activeFiles.get();
            int max = maxActiveFiles.get();
            while (current > max) {
                if (maxActiveFiles.compareAndSet(max, current)) {
                    break;
                }
                max = maxActiveFiles.get();
            }
        }
    }

    public static Report processSequentially(List<LogFile> files, GlobalCounters counters) {
        GlobalAccumulator accumulator = new GlobalAccumulator();

        for (LogFile file : files) {
            try {
                Iterator<String> iterator = LogFileLines.iterator(file.path());
                int lineNumber = 0;

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    lineNumber++;
                    counters.readLines.incrementAndGet();

                    try {
                        LogEntry entry = parseLine(line);
                        accumulator.add(entry.path(), entry);
                        counters.correctEntries.incrementAndGet();
                    } catch (Exception e) {
                        counters.corruptedEntries.incrementAndGet();
                    }
                }

                counters.processedFiles.incrementAndGet();
            } catch (Exception e) {
                counters.readErrors.incrementAndGet();
                counters.failedFiles.add(file.path().toString());
            }
        }

        return accumulator.buildReport();
    }

    public static Report processParallel(
            List<LogFile> files,
            int threadCount,
            int semaphoreLimit,
            GlobalCounters counters
    ) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Semaphore semaphore = new Semaphore(semaphoreLimit);
        CountDownLatch latch = new CountDownLatch(files.size());
        GlobalAccumulator accumulator = new GlobalAccumulator();

        for (LogFile file : files) {
            executor.submit(() -> {
                try {
                    if (stopFlag) {
                        return;
                    }

                    semaphore.acquire();
                    try {
                        counters.activeFiles.incrementAndGet();
                        counters.updateMaxActive();

                        Iterator<String> iterator = LogFileLines.iterator(file.path());
                        int lineNumber = 0;

                        while (iterator.hasNext()) {
                            if (stopFlag) {
                                break;
                            }

                            String line = iterator.next();
                            lineNumber++;
                            counters.readLines.incrementAndGet();

                            try {
                                LogEntry entry = parseLine(line);
                                accumulator.add(entry.path(), entry);
                                counters.correctEntries.incrementAndGet();
                            } catch (Exception e) {
                                counters.corruptedEntries.incrementAndGet();
                            }
                        }

                        counters.processedFiles.incrementAndGet();
                    } finally {
                        counters.activeFiles.decrementAndGet();
                        semaphore.release();
                    }
                } catch (Exception e) {
                    counters.readErrors.incrementAndGet();
                    counters.failedFiles.add(file.path().toString());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        return accumulator.buildReport();
    }

    public static void runTests() throws Exception {
        Path folder = Path.of("logs");

        if (!Files.exists(folder)) {
            System.out.println("Папка logs не найдена. Генерируем тестовые данные...");
            GenerateLogs.generate(folder, 5, 100);
        }
        List<LogFile> files = Files.list(folder)
                .filter(p -> p.toString().endsWith(".log"))
                .map(LogFile::new)
                .toList();

        if (files.isEmpty()) {
            System.out.println("Нет файлов для обработки");
            return;
        }

        System.out.println("Тест 1 Последовательная обработка");
        GlobalCounters seqCounters = new GlobalCounters();
        long startTime = System.currentTimeMillis();
        Report seqReport = processSequentially(files, seqCounters);
        long seqTime = System.currentTimeMillis() - startTime;
        System.out.println("Отчёт: " + seqReport);
        System.out.println("Обработано файлов: " + seqCounters.processedFiles.get());
        System.out.println("Прочитано строк: " + seqCounters.readLines.get());
        System.out.println("Корректных записей: " + seqCounters.correctEntries.get());
        System.out.println("Повреждённых записей: " + seqCounters.corruptedEntries.get());
        System.out.println("Время: " + seqTime + " мс");
        System.out.println();

        System.out.println("Тест 2 Параллельная обработка (1 поток, семафор 1)");
        GlobalCounters par1Counters = new GlobalCounters();
        startTime = System.currentTimeMillis();
        Report par1Report = processParallel(files, 1, 1, par1Counters);
        long par1Time = System.currentTimeMillis() - startTime;

        System.out.println("Отчёт: " + par1Report);
        System.out.println("Обработано файлов: " + par1Counters.processedFiles.get());
        System.out.println("Прочитано строк: " + par1Counters.readLines.get());
        System.out.println("Корректных записей: " + par1Counters.correctEntries.get());
        System.out.println("Повреждённых записей: " + par1Counters.corruptedEntries.get());
        System.out.println("Макс. активных файлов: " + par1Counters.maxActiveFiles.get());
        System.out.println("Активные файлы после завершения = 0: " + (par1Counters.activeFiles.get() == 0));
        System.out.println("Время: " + par1Time + " мс");
        System.out.println();

        System.out.println("Тест 3 Параллельная обработка (4 потока, семафор 4)");
        GlobalCounters par4Counters = new GlobalCounters();
        startTime = System.currentTimeMillis();
        Report par4Report = processParallel(files, 4, 4, par4Counters);
        long par4Time = System.currentTimeMillis() - startTime;

        System.out.println("Отчёт: " + par4Report);
        System.out.println("Обработано файлов: " + par4Counters.processedFiles.get());
        System.out.println("Прочитано строк: " + par4Counters.readLines.get());
        System.out.println("Корректных записей: " + par4Counters.correctEntries.get());
        System.out.println("Повреждённых записей: " + par4Counters.corruptedEntries.get());
        System.out.println("Макс. активных файлов: " + par4Counters.maxActiveFiles.get());
        System.out.println("Активные файлы после завершения = 0: " + (par4Counters.activeFiles.get() == 0));
        System.out.println("Время: " + par4Time + " мс");
        System.out.println();

        System.out.println("Тест 4 Досрочная остановка");
        GlobalCounters stopCounters = new GlobalCounters();
        startTime = System.currentTimeMillis();

        Thread worker = new Thread(() -> {
            try {
                processParallel(files, 4, 4, stopCounters);
            } catch (InterruptedException e) {
                System.out.println("Обработка прервана");
            }
        });
        worker.start();

        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        stopFlag = true;

        try {
            worker.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long stopTime = System.currentTimeMillis() - startTime;

        System.out.println("Обработано файлов: " + stopCounters.processedFiles.get());
        System.out.println("Прочитано строк: " + stopCounters.readLines.get());
        System.out.println("Время: " + stopTime + " мс");
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;
            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );
            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        runTests();

        //System.out.println("Usage: java LogAnalysisAssignment "+ "generate <directory> <files> <lines>");
        //System.out.println("       java LogAnalysisAssignment check");
    }
}
