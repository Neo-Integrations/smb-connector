package org.neointegrations.smb.internal.collection;

import com.hierynomus.msfscc.fileinformation.FileInformation;

import java.util.Iterator;

public class SMBFileIterator  implements Iterator<FileInformation> {

    private Iterator<FileInformation> it;
    private int index;

    public SMBFileIterator(Iterator<FileInformation> it) {
        this.it = it;
        index = 0;
    }

    @Override
    public FileInformation next() {
        if(hasNext()) {
            return it.next();
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public void remove() {
        it.remove();
    }
}
