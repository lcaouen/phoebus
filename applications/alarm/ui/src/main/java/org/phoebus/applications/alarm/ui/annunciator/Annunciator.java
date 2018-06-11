package org.phoebus.applications.alarm.ui.annunciator;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.phoebus.applications.alarm.talk.Annunciation;
import org.phoebus.framework.jobs.NamedThreadFactory;

public class Annunciator
{
    private int threshold = 3;
    private long timeout_secs = 5;
    
    private final CopyOnWriteArrayList<Annunciation> to_annunciate = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Annunciation> message_queue = new CopyOnWriteArrayList<>();
    
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Timer"));
    private final Runnable speaker = () -> annunciate();
    
    public Annunciator( int threshold, long time)
    {
        this.threshold = threshold;
        timeout_secs = time;
    }
    
    public void annunciate(Annunciation a)
    {
        message_queue.add(a);
    }

    private void annunciate()
    {
        System.out.println("Annunciating");
        System.out.println(message_queue.size());
        synchronized(message_queue)
        {
            to_annunciate.addAll(message_queue);
            message_queue.clear();
        }
        
        if (to_annunciate.size() > threshold)
        {
            System.out.println("There were " + to_annunciate.size() + " recieved in the last " + timeout_secs + " seconds");
        }
        else
        {
            for (Annunciation a : to_annunciate)
            {
                System.out.println(a.time_received + " " + a.severity + " " + a.message);
            }
        }
        to_annunciate.clear();
    }
    public void start() 
    {
        timer.scheduleAtFixedRate(speaker, timeout_secs, timeout_secs, TimeUnit.SECONDS);
    }
    
    public void stop()
    {
        timer.shutdown();
    }
}
