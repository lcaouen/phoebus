/*
 * Copyright (C) 2019 European Spallation Source ERIC.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 */

package org.csstudio.display.builder.representation.javafx.widgets;

import javafx.geometry.Dimension2D;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.net.URL;

public class PictureRepresentationTest {

    @Test
    public void testGetViewboxSize(){
        String path = null;
        try {
            path = getPath("interlock.svg");
        } catch (Exception e) {
            fail(e.getMessage());
        }

        PictureRepresentation pictureRepresentation = new PictureRepresentation();

        Dimension2D dimension2D = pictureRepresentation.getViewboxSize(path);

        assertTrue(dimension2D.getWidth() == 106);
        assertTrue(dimension2D.getHeight() == 106);
    }

    @Test
    public void testGetBadViewboxSize(){
        String path = null;
        try {
            path = getPath("interlock_no_viewbox.svg");
        } catch (Exception e) {
            fail(e.getMessage());
        }

        PictureRepresentation pictureRepresentation = new PictureRepresentation();

        assertNull(pictureRepresentation.getViewboxSize(path));
    }

    private String getPath(String resource) throws Exception{
        URL url = getClass().getClassLoader().getResource(resource);
        File file = new File(url.toURI());
        return file.getAbsolutePath();
    }
}
