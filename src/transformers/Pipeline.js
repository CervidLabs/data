/**
 * Pipeline - Encadena múltiples transformadores
 */
export class Pipeline {
  constructor(stages = []) {
    this.stages = stages;
    this.isFitted = false;
  }

  add(stage) {
    this.stages.push(stage);
    return this;
  }

  fit(df) {
    let currentDf = df;
    
    for (const stage of this.stages) {
      if (stage.fit) {
        if (stage instanceof StringIndexer || stage instanceof OneHotEncoder) {
          // Estos necesitan columnas específicas
          if (stage.columns) {
            stage.fit(currentDf, stage.columns);
          }
        } else {
          stage.fit(currentDf);
        }
      }
    }
    
    this.isFitted = true;
    return this;
  }

  transform(df) {
    if (!this.isFitted) {
      throw new Error('Pipeline must be fitted first');
    }
    
    let currentDf = df;
    
    for (const stage of this.stages) {
      if (stage.transform) {
        currentDf = stage.transform(currentDf);
      }
    }
    
    return currentDf;
  }

  fitTransform(df) {
    return this.fit(df).transform(df);
  }
}