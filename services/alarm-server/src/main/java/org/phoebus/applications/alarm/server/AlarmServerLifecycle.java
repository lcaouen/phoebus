package org.phoebus.applications.alarm.server;

import org.phoebus.applications.alarm.client.ClientState;
import org.phoebus.util.shell.CommandShell;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.phoebus.applications.alarm.AlarmSystemConstants.logger;

@Component
public class AlarmServerLifecycle
        implements SmartLifecycle, ServerModelListener {

    private final AlarmServerProperties properties;

    private final LinkedBlockingQueue<Boolean> restartQueue =
            new LinkedBlockingQueue<>();

    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile ServerModel model;
    private volatile CommandShell shell;

    private ExecutorService executor;

    public AlarmServerLifecycle(AlarmServerProperties properties) {
        this.properties = properties;
    }

    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "phoebus-alarm-server");
            thread.setDaemon(false);
            return thread;
        });

        executor.submit(this::runServerLoop);
    }

    private void runServerLoop() {
        try {
            while (running.get()) {
                try {
                    startServerCycle();

                    // Attend une demande de redémarrage
                    // ou d'arrêt.
                    boolean shouldRestart = restartQueue.take();

                    stopServerCycle();

                    if (!shouldRestart) {
                        break;
                    }

                    logger.info("Restarting alarm server...");
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ex) {
                    logger.log(
                            Level.SEVERE,
                            "Alarm server lifecycle error",
                            ex
                    );
                    break;
                }
            }
        } finally {
            stopServerCycle();
            running.set(false);
        }
    }

    private void startServerCycle() throws Exception {
        String server = properties.getKafkaServer();
        String config = properties.getConfig();
        String kafkaProperties = properties.getKafkaProperties();

        logger.info("Server: " + server);
        logger.info("Config: " + config);

        TopicUtils.ensureKafkaTopics(
                server, config, kafkaProperties
        );

        AlarmStateInitializer initializer =
                new AlarmStateInitializer(server, config, kafkaProperties);

        ConcurrentHashMap<String, ClientState> initialStates;

        try {
            if (initializer.awaitCompleteStates()) {
                logger.info("Alarm state stabilized");
            } else {
                logger.warning(
                        "Initial alarm states may be incomplete"
                );
            }
        } finally {
            initialStates = initializer.shutdown();
        }

        model = new ServerModel(
                server,
                config,
                initialStates,
                this,
                kafkaProperties
        );

        model.start();

        if (properties.isShellEnabled()) {
            startShell();
        }

        logger.info("Alarm server started");
    }

    private void startShell() {
        // Réutiliser ici COMMANDS et handleShellCommands(...)
        // de l'ancienne classe.
        //
        // shell = new CommandShell(COMMANDS, this::handleShellCommands);
        // shell.setPrompt(model.getRoot().getPathName());
        // shell.start();
    }

    private synchronized void stopServerCycle() {
        CommandShell currentShell = shell;
        shell = null;

        if (currentShell != null) {
            try {
                currentShell.stop();
            } catch (Exception ex) {
                logger.log(
                        Level.WARNING,
                        "Error stopping command shell",
                        ex
                );
            }
        }

        ServerModel currentModel = model;
        model = null;

        if (currentModel != null) {
            try {
                currentModel.shutdown();
            } catch (Exception ex) {
                logger.log(
                        Level.WARNING,
                        "Error stopping alarm model",
                        ex
                );
            }
        }
    }

    public void requestRestart() {
        if (running.get()) {
            restartQueue.offer(Boolean.TRUE);
        }
    }

    public void requestShutdown() {
        if (running.get()) {
            restartQueue.offer(Boolean.FALSE);
        }
    }

    @Override
    public synchronized void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        restartQueue.offer(Boolean.FALSE);

        if (executor != null) {
            executor.shutdown();

            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException ex) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        stopServerCycle();
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void handleCommand(String path, String json) {
        // Déplacer ici le corps de handleCommand(...)
        // de l'ancienne classe, sans changer la logique métier.
    }
}