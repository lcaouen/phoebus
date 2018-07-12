/*******************************************************************************
 * Copyright (c) 2018 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.phoebus.applications.alarm.server.actions;

import static org.phoebus.applications.alarm.AlarmSystem.logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.logging.Level;

import org.phoebus.applications.alarm.model.AlarmTreeItem;
import org.phoebus.applications.alarm.model.AlarmTreeLeaf;
import org.phoebus.applications.alarm.model.SeverityLevel;
import org.phoebus.applications.alarm.model.TitleDetailDelay;
import org.phoebus.framework.jobs.NamedThreadFactory;


/** Handler of automated actionsAlarm handling logic.
 *
 *  @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class AutomatedActions
{
    /** Timer shared by all automated actions */
    private static final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("AutomatedActions"));

    /** Is the item in active alarm? */
    private final AtomicBoolean active_alarm;

    /** Actions that have been scheduled with the timer */
    private final ConcurrentHashMap<TitleDetailDelay, ScheduledFuture<?>> scheduled_actions = new ConcurrentHashMap<>(1);

    /** Item for which to handle automated actions */
    private final AlarmTreeItem<?> item;

    /** Will be invoked to actually perform one of the item's actions */
    private final BiConsumer<AlarmTreeItem<?>, TitleDetailDelay> perform_action;

    /** Handle automated actions for one item
     *  @param item Item for which automated actions should be handled
     *  @param start_active Start with an active alarm (which is ignored)?
     *  @param perform_action Will be invoked to actually perform the action for an item
     */
    public AutomatedActions(final AlarmTreeItem<?> item,
                            final boolean start_active,
                            final BiConsumer<AlarmTreeItem<?>, TitleDetailDelay> perform_action)
    {
        this.item = item;
        this.active_alarm = new AtomicBoolean(start_active);
        this.perform_action = perform_action;

        // If item is PV, and not enabled, this should never be called
        if (item instanceof AlarmTreeLeaf   &&  ! ((AlarmTreeLeaf) item).isEnabled())
            throw new IllegalStateException();
    }

    /** Handle a severity update
     *
     *  <p>Check if actions need to be scheduled/triggered/cancelled
     *
     *  @param severity Most recent alarm severity of the item
     */
    public void handleSeverityUpdate(final SeverityLevel severity)
    {
        // System.out.println(item.getPathName() + " auto actions update for " + severity);
        final boolean is_active = severity.isActive();
        // Is this a change?
        if (active_alarm.compareAndSet(!is_active, is_active))
        {
            if (is_active)
            {
                for (TitleDetailDelay action : item.getActions())
                {
                    if (action.detail.startsWith(TitleDetailDelay.SEVRPV))
                        continue;

                    // Schedule action to be executed unless already scheduled
                    scheduled_actions.computeIfAbsent(action, a ->
                    {
                        final Runnable trigger_action = () ->
                        {
                            if (scheduled_actions.remove(a) == null)
                                logger.log(Level.INFO, item.getPathName() + ": Aborting execution of cancelled action " + a);
                            else
                                perform_action.accept(item, a);
                        };
                        logger.log(Level.INFO, item.getPathName() + ": Schedule " + a.title + " in " + a.delay + " s");
                        return timer.schedule(trigger_action, a.delay, TimeUnit.SECONDS);
                    });
                }
            }
            else // Cancel all scheduled actions
                cancel();
        }
    }

    public void cancel()
    {
        // Cancel/clear all scheduled actions
        scheduled_actions.forEach((action, scheduled) ->
        {
            scheduled.cancel(false);
            logger.log(Level.INFO, item.getPathName() + ": Cancelled");
            scheduled_actions.remove(action);
        });
    }
}