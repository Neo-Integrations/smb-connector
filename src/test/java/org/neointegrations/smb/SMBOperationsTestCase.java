package org.neointegrations.smb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.After;
import org.junit.Before;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SMBOperationsTestCase extends MuleArtifactFunctionalTestCase {

  private static final Logger _logger = LoggerFactory.getLogger(SMBOperationsTestCase.class);

  /**
   * Specifies the mule config xml with the flows that are going to be executed in the tests, this file lives in the test resources.
   */
  @Override
  protected String getConfigFile() {
    return "test-mule-config.xml";
  }


  @Before
  public void executeInit() throws Exception {
    _logger.info("Calling executeInit...");
    flowRunner("init-flow").run();
  }
  @After
  public void executeDestroy() throws Exception {
    _logger.info("Calling executeInit...");
    flowRunner("destroy-flow").run();
  }

  @Test
  public void executeRead() throws Exception {
    _logger.info("Calling executeRead...");
    Boolean payloadValue = ((Boolean) flowRunner("read-flow").run()
            .getMessage()
            .getPayload()
            .getValue());
    assertThat(payloadValue, is(true));
  }

  @Test
  public void executeWrite() throws Exception {
    _logger.info("Calling executeWrite...");

    Boolean payloadValue = ((Boolean) flowRunner("write-flow").run()
                                      .getMessage()
                                      .getPayload()
                                      .getValue());
    assertThat(payloadValue, is(true));
  }

  @Test
  public void executeList() throws Exception {
    _logger.info("Calling executeList...");
    Boolean payloadValue = ((Boolean) flowRunner("list-flow").run()
            .getMessage()
            .getPayload()
            .getValue());
    assertThat(payloadValue, is(true));
  }

  @Test
  public void executeRemoteCopy() throws Exception {
    _logger.info("Calling executeList...");
    Boolean payloadValue = ((Boolean) flowRunner("remote-copy-flow").run()
            .getMessage()
            .getPayload()
            .getValue());
    assertThat(payloadValue, is(true));
  }

}
