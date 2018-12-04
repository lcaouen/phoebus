/*******************************************************************************
 * Copyright (c) 2010-2018 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.csstudio.display.pace;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.csstudio.display.pace.model.Cell;
import org.csstudio.display.pace.model.Model;
import org.junit.Test;

/** JUnit test of Model
 *
 *  @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class ModelUnitTest
{
    /** Counter for received updates from cells */
    private final AtomicInteger updates = new AtomicInteger(0);
    private final  AtomicInteger values = new AtomicInteger(0);

    /** ModelListener that counts received updates */
    private final Consumer<Cell> listener = cell ->
    {
        updates.incrementAndGet();
        System.out.println("CellUpdate: " + cell);
        String value = cell.getCurrentValue();
        if (value != null  &&  !value.isEmpty())
            values.incrementAndGet();
    };

    /** Check if non-existing model file is detected */
    @Test
    public void testBadModel()
    {
        try
        {
            new Model(new FileInputStream("nonexisting_file.pace"));
        }
        catch (Exception ex)
        {
            // Detected the missing file?
            if (ex instanceof FileNotFoundException  &&
                ex.getMessage().contains("nonexisting_file.pace"))
                return;
            // Else: Didn't get the expected error
            ex.printStackTrace();
        }
        fail("Didn't catch missing file");
    }


    /** Check basic model readout:
     *  Correct title, # of columns, instances, cell's PV names?
     */
    @Test
    public void testModel() throws Exception
    {
        final Model model = new Model(Model.class.getResourceAsStream("/pace_examples/localtest.pace"));

        assertThat(model.getTitle(), equalTo("Demo"));

        assertThat(model.getColumnCount(), equalTo(3));
        assertThat(model.getColumn(0).getName(), equalTo("Setpoint"));
        assertThat(model.getColumn(1).getName(), equalTo("Limit"));

        assertThat(model.getInstanceCount(), equalTo(5));
        assertThat(model.getInstance(0).getName(), equalTo("System 1"));
        assertThat(model.getInstance(0).getCell(0).getName(), equalTo("loc://setpoint1(1)"));

        assertThat(model.getInstance(4).getName(), equalTo("System 5"));
        assertThat(model.getInstance(4).getCell(1).getName(), equalTo("loc://limit5(5)"));
    }

    /** Check editing */
    @Test
    public void testModelEdits() throws Exception
    {
        // Create model that's not actually listening to PV updates
        final Model model = new Model(Model.class.getResourceAsStream("/pace_examples/localtest.pace"));
        model.addListener(listener);

        // Model has not been edited, find a cell to test changes
        // Reset counter
        updates.set(0);

        // Confirm Model has not been edited
        assertFalse(model.isEdited());

        // Assert that we have a non-readonly cell to test
        final Cell cell = model.getInstance(1).getCell(1);
        assertFalse(cell.isReadOnly());

        // Edit the cell
        cell.setUserValue("10");
        // Expect an update, cell and model in "edited" state
        assertThat(updates.get(), equalTo(1));
        assertTrue(cell.isEdited());
        assertTrue(model.isEdited());
        // Cell should reflect the value that we entered via setUserValue
        assertThat(cell.getValue(), equalTo("10"));
        assertThat(cell.getUserValue(), equalTo("10"));

        // Revert to original value
        cell.clearUserValue();
        // Should result in another update, since value changes back
        assertThat(updates.get(), equalTo(2));
        // Confirm that the edited values were replaced with the original
        // and is no longer considered edited
        assertFalse(cell.isEdited());
        assertFalse(model.isEdited());
        assertThat(cell.getUserValue(), nullValue());
    }

    /** Check PV connection. */
    @Test
    public void testModelPVs() throws Exception
    {
        final Model model = new Model(Model.class.getResourceAsStream("/pace_examples/localtest.pace"));
        model.addListener(listener);

        // Reset counter
        updates.set(0);

        // Should not see changes...
        Thread.sleep(3000);
        assertThat(updates.get(), equalTo(0));

        // Connect PVs, so now we expect to receive the current values
        model.start();

        // Give it some time
        for (int sec=0; sec < 10; ++sec)
        {
            if (updates.get() > 0)
                break;
            Thread.sleep(1000);
        }
        model.stop();

        // Even though nobody edited the model...
        assertFalse(model.isEdited());
        // ... should have received a few (initial) updates
        assertTrue(updates.get() > 0);
        // ... should have received a few real values
        assertTrue(values.get() > 0);
    }
}
