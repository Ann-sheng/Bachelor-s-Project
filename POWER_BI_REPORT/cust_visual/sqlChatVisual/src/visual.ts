import powerbi from "powerbi-visuals-api";
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;
import IVisual = powerbi.extensibility.visual.IVisual;

import * as React from "react";
import { createRoot } from "react-dom/client";
import { ChatVisual } from "./ChatVisual";

export class Visual implements IVisual {
    private root: any;

    constructor(options: VisualConstructorOptions) {
        this.root = createRoot(options.element);
        this.root.render(React.createElement(ChatVisual));
    }

    public update(options: VisualUpdateOptions) {
     
    }

    public destroy() {
        this.root.unmount();
    }
}