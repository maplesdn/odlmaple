/**
 * Created by zhushigang on 3/2/15.
 */

package org.opendaylight.l2switch.packethandler.decoders;

import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.maple.core.Controller;

public class ODLController implements Controller {

  public void sendPacket(byte[] data, int... ports) {

    for (int i = 0; i < ports.length; i++) {
      System.out.println("ODLController sending packet to port " + ports[i]);
      // do something with ports[i]
    }

  }

}
