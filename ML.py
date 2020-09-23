import ABC
import torch

# 1) Beat the S&P-500:
# Maximize .5 * Information ratio, .2 * Diversification, and .2 * Robustness.
def loss_beat_index(forward_pass, model, **kwargs):
    pass

class BeatSNP(torch.nn.Module):
    def __init__(self, data, features):
        """
        * Create layers for the model based upon 
        dataset.
        """
        self.__Initialize(data, features)

    def forward(self, input):
        """
        * Input must be a dataframe.
        """
        return self.modules(input)

    def Train(self, epochs, loss = loss_beat_index):
        batches = []
        for epoch in range(0, epochs):
            for batch in batches:
                curr_loss = loss(batch, model)
                self.modules.zero_grad()
                curr_loss.backward()

    def Test(self, epochs):
        """
        * Compute loss on testing set.
        """
        pass

    ####################
    # Private Helpers:
    ####################
    @staticmethod
    def __GetFeatures(data, feats, **featargs):
        X = X.to_dict()
        for feat in feats:
            X = feat.Extract(X, featargs)
        return X

    def __Initialize(self, data, feats, **featargs):
        # Notes:
        # 1) Essentially a selection problem: Select or do not select?
        # So train a binomial distribution where p_is occur at every stage
        # -> Select / NotSelect such that the above success
        # computation is maximized.
        # 2) Need to implement running computation so not forced to recompute
        # the success ratio every iteration, do by adding additional columns to
        # dataset.
        # 3) Add additional strategies.
        # - Strategies: When to drop a loser and select another?
        # i.e. optimal loss to give up and drop from feasible
        # investment set.
        # Extract all features:
        data = BeatSNP.__GetFeatures(X, feats, featargs)
        # Create model based on dataset:
        layers = [torch.nn.Linear()]
        layers.append(torch.nn.LTSM())
        self.modules = torch.nn.Sequential(layers)


# 2) Complex Option Pricing:
def loss_option_pricing(X, **kwargs):
    pass
