package org.neointegrations.smb.internal;

import static org.mule.runtime.api.meta.model.display.PathModel.Location.EXTERNAL;
import static org.mule.runtime.api.meta.model.display.PathModel.Type.DIRECTORY;

import org.mule.extension.file.common.api.source.AbstractPostActionGroup;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.Path;

/**
 * Groups post processing action parameters
 *
 * @since 1.1
 */
public class PostActionGroup extends AbstractPostActionGroup {

    /**
     * Whether each file should be deleted after processing or not
     */
    @Parameter
    @Optional(defaultValue = "false")
    private boolean autoDelete;

    /**
     * If provided, each processed file will be moved to a directory pointed by this path.
     */
    @Parameter
    @Optional
    @Path(type = DIRECTORY, location = EXTERNAL)
    private String moveToDirectory;

    /**
     * This parameter works in tandem with {@code moveToDirectory}. Use this parameter to enter the name under which the file should
     * be moved. Do not set this parameter if {@code moveToDirectory} hasn't been set as well.
     */
    @Parameter
    @Optional
    private String renameTo;

    /**
     * Whether any of the post actions ({@code autoDelete} and {@code moveToDirectory}) should also be applied in case the file
     * failed to be processed. If set to {@code false}, no failed files will be moved nor deleted.
     */
    @Parameter
    @Optional(defaultValue = "true")
    private boolean applyPostActionWhenFailed;

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public String getMoveToDirectory() {
        return moveToDirectory;
    }

    public String getRenameTo() {
        return renameTo;
    }

    @Override
    public boolean isApplyPostActionWhenFailed() {
        return applyPostActionWhenFailed;
    }

}
